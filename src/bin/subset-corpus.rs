/// Executable to subset the OpenCorpus files

use std::io::prelude::*;
use std::path::{PathBuf, Path};
use std::collections::HashSet;
use std::sync::Arc;
use std::mem::drop;
use std::thread;

use structopt::StructOpt;
use anyhow::{Result, anyhow};
use crossbeam::channel::{unbounded, Sender, Receiver};
use threadpool::ThreadPool;
use serde_json::Value;
use indicatif::{MultiProgress, ProgressBar};

use fair_trec_tools::ai2::PaperMetadata;
use fair_trec_tools::io::{open_gzout, open_gzin, make_progress};
use fair_trec_tools::corpus::OpenCorpus;

#[derive(Debug, StructOpt)]
#[structopt(name="subset-corpus")]
struct SubsetCommand {
  /// Path to the output file.
  #[structopt(short="o", long="output-file")]
  output: PathBuf,

  /// Path to the paper metadata as input.
  #[structopt(short="M", long="paper-meta")]
  paper_meta: Option<PathBuf>,

  /// Path to OpenCorpus download directory.
  corpus_path: PathBuf
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
    } else {
      Err(anyhow!("no source of target documents provided."))
    }
  }

  /// Perform the subset operation
  fn subset(&self, targets: &Arc<HashSet<String>>) -> Result<usize> {
    let (tx, rx) = unbounded();
    let out_h = self.writer_thread(rx);
    let mpb = MultiProgress::new();
    let pool = ThreadPool::new(4);
    eprintln!("scanning corpus in {:?}", &self.corpus_path);
    let corpus = OpenCorpus::create(&self.corpus_path);
    let files = corpus.get_files()?;
    eprintln!("found {} corpus files", files.len());
    for file in files {
      let t2 = tx.clone();
      let pb = make_progress();
      let pb = mpb.add(pb);
      let tref = targets.clone();
      pool.execute(move || {
        let res = subset_file(&file, t2, &tref, &pb);
        match res {
          Err(e) => {
            eprintln!("error reading {:?}: {}", &file, e);
            std::process::exit(1);
          },
          Ok((nr, _ns)) => {
            pb.println(format!("scanned {} records from {:?}", nr, &file));
          }
        }
      });
    }
    eprintln!("work queued, let's go!");
    mpb.join()?;
    pool.join();
    // send end-of-data sentinel
    tx.send(Value::Null)?;
    drop(tx);

    // unwrap propagates panics, ? propagates IO errors
    let n = out_h.join().unwrap()?;
    Ok(n)
  }

  /// Create a writer thread to write subset documents to disk.
  fn writer_thread(&self, rx: Receiver<Value>) -> thread::JoinHandle<Result<usize>> {
    // write output in a thread
    let outf = self.output.to_owned();
    thread::spawn(move || {
      let mut n = 0;
      let mut output = open_gzout(&outf)?;
      let mut done = false;
      while !done {
        let msg = rx.recv()?;
        match msg {
          Value::Null => done = true,
          m => {
            n += 1;
            write!(&mut output, "{}\n", m)?;
          }
        };
      }
      Ok(n)
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

/// Subset a file of corpus results into a recipient.
pub fn subset_file(src: &Path, out: Sender<Value>, targets: &HashSet<String>, pb: &ProgressBar) -> Result<(usize, usize)> {
  let mut read = 0;
  let mut sent = 0;
  let src = open_gzin(src, pb)?;

  for line in src.lines() {
    let ls = line?;
    let val: Value = serde_json::from_str(&ls)?;
    read += 1;
    if let Some(Value::String(id)) = val.get("id") {
      if targets.contains(id) {
        sent += 1;
        out.send(val)?;
      }
    }
  }

  Ok((read, sent))
}
