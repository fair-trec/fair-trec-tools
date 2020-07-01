use std::collections::HashSet;
use std::path::Path;

use anyhow::Result;

use crate::ai2::{PaperMetadata};
use crate::queries::QueryRecord;

/// Type for a set of document IDs.
pub struct DocSet {
  pub ids: HashSet<String>
}

impl DocSet {
  pub fn new() -> DocSet {
    DocSet {
      ids: HashSet::new()
    }
  }

  /// Read the list of desired paper IDs from metadata
  pub fn load_metadata(&mut self, path: &Path) -> Result<usize> {
    eprintln!("reading target documents from {:?}", path);
    let papers = PaperMetadata::read_csv(path)?;
    let init_size = self.ids.len();
    for paper in papers.iter() {
      self.ids.insert(paper.paper_sha.clone());
    }
    let added = self.ids.len() - init_size;
    Ok(added)
  }

  /// Read the list of desired paper IDs from metadata JSON
  pub fn load_json_queries(&mut self, path: &Path) -> Result<usize> {
    eprintln!("reading target documents from {:?}", path);
    let queries = QueryRecord::read_jsonl(path)?;
    self.load_queries(queries)
  }

  /// Read the list of desired paper IDs from metadata CSV
  pub fn load_csv_queries(&mut self, path: &Path) -> Result<usize> {
    eprintln!("reading target documents from {:?}", path);
    let queries = QueryRecord::read_csv(path)?;
    self.load_queries(queries)
  }

  /// Process a list of documents into a metadata CSV
  pub fn load_queries(&mut self, queries: Vec<QueryRecord>) -> Result<usize> {
    let init_size = self.ids.len();
    for query in queries.iter() {
      for qdoc in &query.documents {
        self.ids.insert(qdoc.doc_id.clone());
      }
    }
    let added = self.ids.len() - init_size;
    Ok(added)
  }
}
