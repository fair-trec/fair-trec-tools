use std::path::{Path, PathBuf};
use std::thread;
use std::sync::Arc;
use std::collections::HashSet;

use crossbeam::channel::Receiver;

use crate::prelude::*;

use crate::author::AuthTbl;
use crate::corpus::Paper;
use super::csv_path;

pub struct AuthorMsg {
  pub paper: Arc<Paper>,
  pub keep: bool
}

/// Create a worker thread to process authors
pub fn writer_thread<P: AsRef<Path>>(path: P, rx: Receiver<AuthorMsg>) -> thread::JoinHandle<Result<usize>> {
  let path: &Path = path.as_ref();
  let outf = path.to_owned();

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

/// Worker procedure for handling authors
fn author_worker(outf: PathBuf, rx: Receiver<AuthorMsg>) -> Result<usize> {
  let mut table = AuthTbl::new();
  let mut auth_set = HashSet::new();

  for rec in rx {
    table.record_paper(&rec.paper);
    if rec.keep {
      for auth in &rec.paper.authors {
        if let Some(id) = auth.id() {
          auth_set.insert(id);
        }
      }
    }
  }

  eprintln!("writing {} authors", auth_set.len());
  let mut auth_out = csv::Writer::from_path(&csv_path(&outf, "authors")?)?;
  let mut n = 0;
  for aid in auth_set {
    match table.lookup(aid) {
      Some(auth) => {
        auth_out.serialize(&auth)?;
        n += 1;
      },
      None => {
        eprintln!("unknown author {}", aid);
      }
    }
  }
  eprintln!("wrote {} authors", n);
  Ok(n)
}
