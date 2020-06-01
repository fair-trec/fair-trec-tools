/// This module contains logic for reading the AI2-released files.

use std::path::Path;
use std::fs::File;

use anyhow::Result;
use serde::{Serialize, Deserialize};

use crate::io::make_progress;

/// Paper metadata from the AI2 CSV file.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PaperMetadata {
  pub paper_sha: String,
  pub paper_title: String,
  pub paper_year: Option<f32>,
  pub paper_venue: String,
  pub n_citations: Option<f32>,
  pub n_key_citations: Option<f32>
}

impl PaperMetadata {
  /// Read in paper metadata from a CSV file.
  pub fn read_csv<P: AsRef<Path>>(path: P) -> Result<Vec<PaperMetadata>> {
    let mut papers = Vec::new();
    let file = File::open(path)?;
    let pb = make_progress();
    pb.set_length(file.metadata()?.len());
    pb.set_prefix("metadata");
    let pbr = pb.wrap_read(file);
    let mut read = csv::Reader::from_reader(pbr);

    for line in read.deserialize() {
      let record: PaperMetadata = line?;
      papers.push(record);
    }

    Ok(papers)
  }
}
