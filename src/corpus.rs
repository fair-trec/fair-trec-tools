// OpenCorpus routines

use std::path::{Path, PathBuf};
use std::fs::read_dir;

use anyhow::Result;
use regex::Regex;

/// Encapsulate an OpenCorpus instance.
pub struct OpenCorpus {
  pub path: PathBuf
}

impl OpenCorpus {
  /// Create an object referencing a corpus download a path.
  pub fn create<P: AsRef<Path>>(path: P) -> OpenCorpus {
    OpenCorpus {
      path: path.as_ref().to_owned()
    }
  }

  /// Get the files in an OpenCorpus download.
  pub fn get_files(&self) -> Result<Vec<PathBuf>> {
    let pat = Regex::new(r"^s2-corpus-\d\d*\.gz").unwrap();
    let mut files = Vec::new();
    // scan directory for children that match the pattern
    for kid in read_dir(&self.path)? {
      let kid = kid?;
      let name = kid.file_name();
      let name = name.to_str().unwrap();
      if pat.is_match(name) {
        files.push(kid.path().to_path_buf());
      }
    }

    Ok(files)
  }
}
