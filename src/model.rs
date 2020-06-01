use serde::{Serialize, Deserialize};

/// Data structure representing a paper from Semantic Scholar Open Corpus.
///
/// This stores information about a single paper. Serde is fine with JSON having
/// more fields than a data structure requires, so we only define here the fields
/// we are going to play with.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Paper {
  pub id: String,
  pub doi: String,
  pub title: String,
  pub sources: Vec<String>,
  pub authors: Vec<Author>,
  #[serde(rename="outCitations")]
  pub out_citations: Vec<String>,
  #[serde(rename="inCitations")]
  pub in_citations: Vec<String>,
  #[serde(rename="pdfUrls")]
  pub pdf_urls: Vec<String>
}

/// Data structure representing an encounter with an author
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Author {
  pub name: String,
  pub ids: Vec<String>
}

impl Author {
  pub fn num_ids(&self) -> usize {
    self.ids.len()
  }
}

/// Data structure for paper metadta from the AI2 CSV file.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PaperMetadata {
  paper_sha: String,
  paper_title: String,
  paper_year: f32,
  paper_venue: String, 
  n_citations: f32,
  n_key_citations: f32
}