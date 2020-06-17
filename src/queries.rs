/// Logic for parsing query data.

use std::io::prelude::*;
use std::path::Path;
use std::fs::File;
use std::io::BufReader;

use anyhow::{Result, anyhow};
use serde::{Serialize, Deserialize};
use csv::ReaderBuilder;

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
  /// Read in queries from a JSON lines file.
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

  /// Read in queries from a ragged CSV file.
  pub fn read_csv<P: AsRef<Path>>(path: P) -> Result<Vec<QueryRecord>> {
    let mut queries = Vec::new();

    let file = File::open(path)?;
    let pb = make_progress();
    pb.set_length(file.metadata()?.len());
    pb.set_prefix("queries");
    let pbr = pb.wrap_read(file);

    let mut read = ReaderBuilder::new().flexible(true).has_headers(false).from_reader(pbr);
    for rec in read.records() {
      let rec = rec?;
      if rec.len() < 3 {
        return Err(anyhow!("CSV line too short"));
      }
      let qid: u64 = rec[0].parse()?;
      let query = rec[1].to_owned();
      let freq: f64 = rec[2].parse()?;
      let doc_base = 3;
      let rem = rec.len() - 3;
      let ndocs = rem / 2;
      let rel_base = doc_base + ndocs;
      if rel_base + ndocs > rec.len() {
        return Err(anyhow!("CSV line too short: has {} cols, expected {}", rec.len(), rel_base + ndocs));
      }

      let mut docs = Vec::with_capacity(ndocs);

      for i in 0..ndocs {
        let q = &rec[doc_base + i];
        let qrel: f64 = rec[rel_base + 1].parse()?;
        docs.push(QueryDoc {
          doc_id: q.to_owned(),
          relevance: qrel
        });
      }

      queries.push(QueryRecord {
        qid: qid,
        query: query,
        frequency: freq,
        documents: docs
      });
    }

    Ok(queries)
  }
}
