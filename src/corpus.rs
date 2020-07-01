// OpenCorpus routines

use std::path::{Path, PathBuf};
use std::fs::read_dir;
use std::convert::TryFrom;

use anyhow::Result;
use regex::Regex;

use serde::{Serialize, Deserialize};
use crate::ai2;

/// Encapsulate an OpenCorpus instance.
pub struct OpenCorpus {
  pub path: PathBuf
}

/// Encapsulate a paper
#[derive(Debug, Serialize, Deserialize, Clone)]
#[allow(non_snake_case)]
pub struct Paper {
  pub id: String,
  pub title: String,
  pub paperAbstract: Option<String>,
  pub year: Option<u32>,
  pub venue: Option<String>,
  pub inCitations: Vec<String>,
  pub outCitations: Vec<String>,
  pub doi: Option<String>,
  pub magId: Option<String>,
  pub authors: Vec<PaperAuthor>
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(try_from="PARaw")]
pub struct PaperAuthor {
  pub ids: Vec<i64>,
  pub name: String
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct PARaw {
  pub ids: Vec<String>,
  pub name: String
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

impl Paper {
  pub fn meta(&self) -> ai2::PaperMetadata {
    ai2::PaperMetadata {
      paper_sha: self.id.clone(),
      paper_title: self.title.clone(),
      paper_year: self.year.map(|y| y as f32),
      paper_venue: self.venue.clone().unwrap_or("".to_owned()),
      n_citations: Some(self.inCitations.len() as f32),
      n_key_citations: None
    }
  }

  pub fn meta_authors(&self) -> Vec<ai2::PALink> {
    let mut authors = Vec::with_capacity(self.authors.len());
    for i in 0..self.authors.len() {
      let a = &self.authors[i];
      let aid = a.id();
      authors.push(ai2::PALink {
        paper_sha: self.id.clone(),
        corpus_author_id: aid,
        position: i + 1
      });
    }
    authors
  }
}

impl PaperAuthor {
  pub fn id(&self) -> Option<i64> {
    if self.ids.len() > 0 {
      Some(self.ids[0])
    } else {
      None
    }
  }
}

impl TryFrom<PARaw> for PaperAuthor {
  type Error = anyhow::Error;

  fn try_from(raw: PARaw) -> Result<PaperAuthor> {
    let mut aids = Vec::with_capacity(raw.ids.len());
    for id in raw.ids {
      aids.push(id.parse()?);
    }
    Ok(PaperAuthor {
      ids: aids,
      name: raw.name
    })
  }
}
