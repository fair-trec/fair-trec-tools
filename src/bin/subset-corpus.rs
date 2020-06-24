/// Executable to subset the OpenCorpus files

use std::io::prelude::*;
use std::path::{PathBuf, Path};
use std::collections::HashSet;
use std::sync::Arc;
use std::mem::drop;
use std::thread;

use structopt::StructOpt;
use anyhow::{Result, anyhow};
use crossbeam::channel::{bounded, Sender, Receiver};
use threadpool::ThreadPool;
use serde_json::{Value, from_value};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle, ProgressDrawTarget};
use regex::Regex;

use fair_trec_tools::ai2::PaperMetadata;
use fair_trec_tools::queries::QueryRecord;
use fair_trec_tools::io::{open_gzout, open_gzin, make_progress};
use fair_trec_tools::corpus::{OpenCorpus, Paper};
use fair_trec_tools::author::{AuthTbl};

#[derive(Debug, StructOpt)]
#[structopt(name="subset-corpus")]
struct SubsetCommand {
  /// Path to the output file.
  #[structopt(short="o", long="output-file")]
  output: PathBuf,

  /// Path to the paper metadata as input.
  #[structopt(short="M", long="paper-meta")]
  paper_meta: Option<PathBuf>,

  /// Path to the query data as input.
  #[structopt(short="Q", long="queries")]
  queries: Option<PathBuf>,

  /// Path to query data in internal CSV format
  #[structopt(long="query-csv")]
  query_csv: Option<PathBuf>,

  /// Number of input files to process in parallel
  #[structopt(short="j", long="jobs")]
  n_jobs: Option<usize>,

  /// Path to OpenCorpus download directory.
  corpus_path: PathBuf
}

enum ATMsg {
  Paper (Value, bool),
  Finish
}

fn csv_path<P: AsRef<Path>>(path: P, key: &str) -> Result<PathBuf> {
  let path = path.as_ref();
  let mut copy = path.to_owned();
  let stem = path.file_stem().and_then(|s| s.to_str()).ok_or(anyhow!("non-unicode file name"))?;
  let re = Regex::new(r"\.jsonl?$")?;
  let stem = re.replace(stem, "");
  copy.set_file_name(format!("{}.{}.csv", stem, key));
  Ok(copy)
}

fn main() -> Result<()> {
  let cmd = SubsetCommand::from_args();
  let targets = cmd.get_target_docs()?;
  let targets = Arc::new(targets);

  eprintln!("looking for {} documents", targets.len());
  let found = cmd.subset(&targets)?;
  eprintln!("found {} of {} target documents", found, targets.len());
  Ok(())
}

impl SubsetCommand {
  /// Get the target document IDs
  fn get_target_docs(&self) -> Result<HashSet<String>> {
    if let Some(ref path) = &self.paper_meta {
      tgt_ids_from_metdata(path.as_ref())
    } else if let Some(ref path) = &self.queries {
      tgt_ids_from_queries(path.as_ref(), false)
    } else if let Some(ref path) = &self.query_csv {
      tgt_ids_from_queries(path.as_ref(), true)
    } else {
      Err(anyhow!("no source of target documents provided."))
    }
  }

  /// Perform the subset operation
  fn subset(&self, targets: &Arc<HashSet<String>>) -> Result<usize> {
    let (tx, rx) = bounded(1000);
    let (tx2, rx2) = bounded(1000);
    let out_h = self.writer_thread(rx);
    let a_h = self.author_thread(rx2);
    let mpb = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(2));
    let pool = self.open_pool();
    eprintln!("scanning corpus in {:?}", &self.corpus_path);
    let corpus = OpenCorpus::create(&self.corpus_path);
    let files = corpus.get_files()?;
    eprintln!("found {} corpus files", files.len());
    let fpb = ProgressBar::new(files.len() as u64);
    let fpb = mpb.add(fpb);
    fpb.set_prefix("files");
    let stype = ProgressStyle::default_bar().template("{prefix:16}: {bar:25} {pos}/{len} (eta {eta})");
    fpb.set_style(stype);
    for file in files {
      let w_tx = tx.clone();
      let a_tx = tx2.clone();
      let fpb2 = fpb.clone();
      let pb = make_progress();
      let pb = mpb.add(pb);
      let tref = targets.clone();
      pool.execute(move || {
        pb.reset();
        let res = subset_file(&file, w_tx, a_tx, &tref, &pb);
        match res {
          Err(e) => {
            eprintln!("error reading {:?}: {}", &file, e);
            std::process::exit(1);
          },
          Ok((nr, _ns)) => {
            fpb2.println(format!("scanned {} records from {:?}", nr, &file));
            fpb2.inc(1);
          }
        }
      });
    }
    fpb.println("work queued, let's go!");
    drop(fpb);
    mpb.join_and_clear()?;
    pool.join();
    // send end-of-data sentinel
    tx.send(Value::Null)?;
    tx2.send(ATMsg::Finish)?;
    drop(tx);
    drop(tx2);

    eprintln!("waiting for writer to finish");
    // unwrap propagates panics, ? propagates IO errors
    let n = out_h.join().unwrap()?;
    a_h.join().unwrap()?;
    Ok(n)
  }

  fn open_pool(&self) -> ThreadPool {
    let n = self.n_jobs.unwrap_or(2);
    eprintln!("using {} threads", n);
    ThreadPool::new(n)
  }

  /// Create a writer thread to write subset documents to disk.
  fn writer_thread(&self, rx: Receiver<Value>) -> thread::JoinHandle<Result<usize>> {
    // write output in a thread
    let outf = self.output.to_owned();

    thread::spawn(move || {
      match write_worker(outf, rx) {
        Ok(n) => Ok(n),
        Err(e) => {
          eprintln!("writer thread failed: {:?}", e);
          Err(e)
        }
      }
    })
  }

  /// Create a worker thread to process authors
  fn author_thread(&self, rx: Receiver<ATMsg>) -> thread::JoinHandle<Result<usize>> {
    // write output in a thread
    let outf = self.output.to_owned();

    thread::spawn(move || {
      match author_worker(outf, rx) {
        Ok(n) => Ok(n),
        Err(e) => {
          eprintln!("writer thread failed: {:?}", e);
          Err(e)
        }
      }
    })
  }
}

/// Read the list of desired paper IDs from metadata
fn tgt_ids_from_metdata(path: &Path) -> Result<HashSet<String>> {
  eprintln!("reading target documents from {:?}", path);
  let papers = PaperMetadata::read_csv(path)?;
  let mut ids = HashSet::with_capacity(papers.len());
  for paper in papers.iter() {
    ids.insert(paper.paper_sha.clone());
  }
  Ok(ids)
}

/// Read the list of desired paper IDs from metadata
fn tgt_ids_from_queries(path: &Path, csv: bool) -> Result<HashSet<String>> {
  eprintln!("reading target documents from {:?}", path);
  let queries = if csv {
    QueryRecord::read_csv(path)?
  } else {
    QueryRecord::read_jsonl(path)?
  };
  let mut ids = HashSet::new();
  for query in queries.iter() {
    for qdoc in &query.documents {
      ids.insert(qdoc.doc_id.clone());
    }
  }
  Ok(ids)
}

/// Subset a file of corpus results into a recipient.
fn subset_file(src: &Path, out: Sender<Value>, aout: Sender<ATMsg>, targets: &HashSet<String>, pb: &ProgressBar) -> Result<(usize, usize)> {
  let mut read = 0;
  let mut sent = 0;
  let src = open_gzin(src, pb)?;

  for line in src.lines() {
    let ls = line?;
    let val: Value = serde_json::from_str(&ls)?;
    read += 1;
    let mut keep = false;
    if let Some(Value::String(id)) = val.get("id") {
      if targets.contains(id) {
        sent += 1;
        keep = true;
        out.send(val.clone())?;
      }
    }
    aout.send(ATMsg::Paper(val, keep))?;
  }

  Ok((read, sent))
}

/// Worker procedure for doing the writing
fn write_worker(outf: PathBuf, rx: Receiver<Value>) -> Result<usize> {
  let mut n = 0;
  let mut output = open_gzout(&outf)?;
  let mut csv_out = csv::Writer::from_path(&csv_path(&outf, "papers")?)?;
  let mut pal_out = csv::Writer::from_path(&csv_path(&outf, "paper_authors")?)?;
  let mut done = false;
  while !done {
    let msg = rx.recv()?;
    match msg {
      Value::Null => done = true,
      m => {
        n += 1;
        write!(&mut output, "{}\n", m)?;
        let paper: Paper = from_value(m)?;
        let meta = paper.meta();
        csv_out.serialize(&meta)?;
        for pal in paper.meta_authors() {
          pal_out.serialize(&pal)?;
        }
      }
    };
  }

  Ok(n)
}

/// Worker procedure for handling authors
fn author_worker(outf: PathBuf, rx: Receiver<ATMsg>) -> Result<usize> {
  let mut table = AuthTbl::new();
  let mut auth_set = HashSet::new();

  let mut done = false;
  while !done {
    let msg = rx.recv()?;
    match msg {
      ATMsg::Finish => done = true,
      ATMsg::Paper (m, keep) => {
        let paper: Paper = from_value(m)?;
        table.record_paper(&paper);
        if keep {
          for auth in &paper.authors {
            if auth.ids.len() > 1 {
              auth_set.insert(auth.ids[0]);
            }
          }
        }
      }
    };
  }

  eprintln!("writing authors");
  let mut auth_out = csv::Writer::from_path(&csv_path(&outf, "authors")?)?;
  let mut n = 0;
  for aid in auth_set {
    let key = aid.to_string();
    match table.lookup(&key) {
      Some(auth) => {
        auth_out.serialize(&auth)?;
        n += 1;
      },
      None => {
        eprintln!("unknown author {}", aid);
      }
    }
  }
  Ok(n)
}
