/// Logic for parsing query data.

use std::io::prelude::*;
use std::path::Path;
use std::fs::File;
use std::io::BufReader;

use anyhow::Result;
use serde::{Serialize, Deserialize};

use crate::io::make_progress;

/// Query record from the TREC JSON file
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryRecord {
  pub qid: u64,
  pub query: String,
  pub frequency: f64,
  pub documents: Vec<QueryDoc>
}

/// Document in a TREC JSON query record
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryDoc {
  pub doc_id: String,
  #[serde(default)]
  pub relevance: f64
}

impl QueryRecord {
  /// Read in paper metadata from a CSV file.
  pub fn read_jsonl<P: AsRef<Path>>(path: P) -> Result<Vec<QueryRecord>> {
    let mut queries = Vec::new();
    let file = File::open(path)?;
    let pb = make_progress();
    pb.set_length(file.metadata()?.len());
    pb.set_prefix("queries");
    let pbr = pb.wrap_read(file);
    let read = BufReader::new(pbr);

    for line in read.lines() {
      let line = line?;
      let record: QueryRecord = serde_json::from_str(&line)?;
      queries.push(record);
    }

    Ok(queries)
  }
}
