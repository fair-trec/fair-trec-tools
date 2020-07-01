use std::thread::JoinHandle;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::mem::drop;

use regex::Regex;
use crossbeam::channel::{Sender, bounded};

use crate::prelude::*;
use crate::corpus::Paper;

mod author;
mod paper;

use author::AuthorMsg;
use paper::PaperMsg;

fn csv_path<P: AsRef<Path>>(path: P, key: &str) -> Result<PathBuf> {
  let path = path.as_ref();
  let mut copy = path.to_owned();
  let stem = path.file_stem().and_then(|s| s.to_str()).ok_or(anyhow!("non-unicode file name"))?;
  let re = Regex::new(r"\.jsonl?$")?;
  let stem = re.replace(stem, "");
  copy.set_file_name(format!("{}.{}.csv", stem, key));
  Ok(copy)
}

pub trait WritePaper {
  fn write_paper(&self, paper: Paper, json: String, keep: bool) -> Result<()>;
}

pub struct SubsetOutput {
  paper_h: JoinHandle<Result<usize>>,
  paper_tx: Sender<PaperMsg>,

  author_h: JoinHandle<Result<usize>>,
  author_tx: Sender<AuthorMsg>
}

#[derive(Clone)]
pub struct SubsetSink {
  paper_tx: Sender<PaperMsg>,
  author_tx: Sender<AuthorMsg>
}

impl SubsetOutput {
  pub fn create(path: &Path) -> Result<SubsetOutput> {
    let (p_tx, p_rx) = bounded(1000);
    let (a_tx, a_rx) = bounded(1000);
    let p_th = paper::writer_thread(path, p_rx);
    let a_th = author::writer_thread(path, a_rx);
    Ok(SubsetOutput {
      paper_h: p_th,
      paper_tx: p_tx,
      author_h: a_th,
      author_tx: a_tx
    })
  }

  /// Create a hande for a thread to write to this ouput.
  pub fn make_handle(&self) -> SubsetSink {
    SubsetSink {
      paper_tx: self.paper_tx.clone(),
      author_tx: self.author_tx.clone()
    }
  }

  pub fn shutdown(self) -> Result<usize> {
    // disconnect the channels
    drop(self.paper_tx);
    drop(self.author_tx);
    // join the threads
    eprintln!("waiting for paper writer to finish");
    // unwrap propagates panics, ? propagates IO errors
    let n = self.paper_h.join().unwrap()?;
    eprintln!("waiting for author writer to finish");
    self.author_h.join().unwrap()?;
    Ok(n)
  }
}

impl WritePaper for SubsetSink {
  fn write_paper(&self, paper: Paper, json: String, keep: bool) -> Result<()> {
    let paper = Arc::new(paper);
    if keep {
      self.paper_tx.send(PaperMsg {
        paper: paper.clone(),
        json: json
      })?;
    }
    self.author_tx.send(AuthorMsg {
      paper: paper,
      keep: keep
    })?;
    Ok(())
  }
}
