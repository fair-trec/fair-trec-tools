use std::collections::HashMap;

use serde::{Serialize,Deserialize};

use crate::corpus::{Paper,PaperAuthor};

const H_THRESHOLD: usize = 10;

/// Author info accumulator
pub struct AuthAccum {
  pub names: HashMap<String,usize>,
  pub i10: usize,
  pub cite_counts: Vec<usize>
}

impl Default for AuthAccum {
  fn default() -> AuthAccum {
    AuthAccum {
      names: HashMap::new(),
      i10: 0,
      cite_counts: Vec::new()
    }
  }
}

pub struct AuthTbl (HashMap<i64,AuthAccum>);

impl AuthTbl {
  pub fn new() -> AuthTbl {
    AuthTbl(HashMap::new())
  }

  pub fn record_author(&mut self, auth: &PaperAuthor, paper: &Paper) {
    for id in &auth.ids {
      let mut acc = self.0.entry(*id).or_default();
      *acc.names.entry(auth.name.clone()).or_default() += 1;
      let ncites = paper.inCitations.len();
      if ncites >= 10 {
        acc.i10 += 1;
      }
      acc.cite_counts.push(ncites);
    }
  }

  pub fn record_paper(&mut self, paper: &Paper) {
    for ref auth in &paper.authors {
      self.record_author(auth, paper);
    }
  }

  pub fn lookup(&self, aid: i64) -> Option<AuthRec> {
    self.0.get(&aid).map(|acc| AuthRec::create(aid, &acc))
  }
}

/// Author record
#[derive(Serialize, Deserialize)]
pub struct AuthRec {
  corpus_author_id: i64,
  name: String,
  num_citations: usize,
  num_papers: usize,
  i10: usize,
  h_index: usize,
  h_class: &'static str
}

impl AuthRec {
  pub fn create(id: i64, acc: &AuthAccum) -> AuthRec {
    // pick most-used name
    let (_n, name) = acc.names.iter().fold((0, ""), |(cb, nb), (n, c)| {
      if c > &cb {
        (*c, n)
      } else {
        (cb, nb)
      }
    });
    // compute h-index
    let mut cites = acc.cite_counts.clone();
    cites.sort();
    cites.reverse();
    let mut h = 0;
    for i in 0..cites.len() {
      if (cites[i] as usize) >= i {
        h += 1;
      } else {
        break;
      }
    }

    AuthRec {
      corpus_author_id: id,
      name: name.to_string(),
      num_citations: acc.cite_counts.iter().sum(),
      num_papers: acc.cite_counts.len(),
      i10: acc.i10,
      h_index: h,
      h_class: if h < H_THRESHOLD { "L" } else { "H" }
    }
  }
}
