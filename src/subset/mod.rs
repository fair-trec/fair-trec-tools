use std::io::prelude::*;
use std::sync::Arc;

mod docset;
mod sink;

use indicatif::{ProgressBar, MultiProgress, ProgressDrawTarget, ProgressStyle};
use threadpool::ThreadPool;

use crate::prelude::*;
use crate::io::{open_gzin, make_progress};
use crate::corpus::{OpenCorpus, Paper};

pub use docset::DocSet;
pub use sink::{SubsetOutput, SubsetSink, WritePaper};

pub fn subset_corpus(corpus: &OpenCorpus, sink: &SubsetOutput, targets: &Arc<DocSet>, pool: &ThreadPool) -> Result<()> {
  let mpb = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(2));
  eprintln!("scanning corpus in {:?}", &corpus.path);
  let files = corpus.get_files()?;
  eprintln!("found {} corpus files", files.len());

  // set up file progress bar
  let fpb = ProgressBar::new(files.len() as u64);
  let fpb = mpb.add(fpb);
  fpb.set_prefix("files");
  let stype = ProgressStyle::default_bar().template("{prefix:16}: {bar:25} {pos}/{len} (eta {eta})");
  fpb.set_style(stype);

  // scan the files
  for file in files {
    // clone progress bar so we can tick it in the thread
    let fpb2 = fpb.clone();
    // set up this file's progress bar
    let pb = make_progress();
    let pb = mpb.add(pb);
    // set up output handle & clone the targets
    let outh = sink.make_handle();
    let tref = targets.clone();
    pool.execute(move || {
      pb.reset();
      let res = subset_file(&file, &outh, &tref, &pb);
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
  Ok(())
}

/// Subset a file of corpus results into a recipient.
fn subset_file(src: &Path, out: &SubsetSink, targets: &DocSet, pb: &ProgressBar) -> Result<(usize, usize)> {
  let mut read = 0;
  let mut sent = 0;
  let src = open_gzin(src, pb)?;

  for line in src.lines() {
    let json = line?;
    let paper: Paper = serde_json::from_str(&json)?;
    read += 1;
    let keep = targets.ids.contains(&paper.id);
    if keep {
      sent += 1;
    }
    out.write_paper(paper, json, keep)?;
  }

  Ok((read, sent))
}
