use std::sync::Arc;
use std::thread;
use std::path::{Path, PathBuf};

use crossbeam::channel::Receiver;

use crate::prelude::*;
use crate::corpus::Paper;
use crate::io::open_gzout;
use super::csv_path;

pub struct PaperMsg {
  pub paper: Arc<Paper>,
  pub json: String
}

/// Create a writer thread to write subset documents to disk.
pub fn writer_thread(path: &Path, rx: Receiver<PaperMsg>) -> thread::JoinHandle<Result<usize>> {
  // write output in a thread
  let outf = path.to_owned();

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

/// Worker procedure for doing the writing
fn write_worker(outf: PathBuf, rx: Receiver<PaperMsg>) -> Result<usize> {
  let mut n = 0;
  let mut output = open_gzout(&outf)?;
  let mut csv_out = csv::Writer::from_path(&csv_path(&outf, "papers")?)?;
  let mut pal_out = csv::Writer::from_path(&csv_path(&outf, "paper_authors")?)?;
  for msg in rx {
    n += 1;
    write!(&mut output, "{}\n", msg.json)?;
    let meta = msg.paper.meta();
    csv_out.serialize(&meta)?;
    for pal in msg.paper.meta_authors() {
      pal_out.serialize(&pal)?;
    }
  }

  Ok(n)
}
