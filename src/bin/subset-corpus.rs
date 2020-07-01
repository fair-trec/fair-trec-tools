/// Executable to subset the OpenCorpus files

use std::path::PathBuf;
use std::sync::Arc;

use structopt::StructOpt;
use anyhow::{Result, anyhow};
use threadpool::ThreadPool;

use fair_trec_tools::corpus::OpenCorpus;
use fair_trec_tools::subset::*;

#[derive(Debug, StructOpt)]
#[structopt(name="subset-corpus")]
struct SubsetCommand {
  /// Path to the output file.
  #[structopt(short="o", long="output-file")]
  output: PathBuf,

  /// Path to the paper metadata as input.
  #[structopt(short="M", long="paper-meta")]
  paper_meta: Vec<PathBuf>,

  /// Path to the query data as input.
  #[structopt(short="Q", long="queries")]
  queries: Vec<PathBuf>,

  /// Path to query data in internal CSV format
  #[structopt(long="query-csv")]
  query_csv: Vec<PathBuf>,

  /// Number of input files to process in parallel
  #[structopt(short="j", long="jobs")]
  n_jobs: Option<usize>,

  /// Path to OpenCorpus download directory.
  corpus_path: PathBuf
}

fn main() -> Result<()> {
  let cmd = SubsetCommand::from_args();
  let targets = cmd.get_target_docs()?;
  let targets = Arc::new(targets);

  eprintln!("looking for {} documents", targets.ids.len());
  let found = cmd.subset(&targets)?;
  eprintln!("found {} of {} target documents", found, targets.ids.len());
  Ok(())
}

impl SubsetCommand {
  /// Get the target document IDs
  fn get_target_docs(&self) -> Result<DocSet> {
    let mut docs = DocSet::new();
    for path in &self.paper_meta {
      docs.load_metadata(path.as_ref())?;
    }
    for path in &self.queries {
      docs.load_json_queries(path.as_ref())?;
    }
    for path in &self.query_csv {
      docs.load_csv_queries(path.as_ref())?;
    }
    if docs.ids.is_empty() {
      Err(anyhow!("no source of target documents provided."))
    } else {
      Ok(docs)
    }
  }

  /// Perform the subset operation
  fn subset(&self, targets: &Arc<DocSet>) -> Result<usize> {
    let out = SubsetOutput::create(&self.output)?;
    let corpus = OpenCorpus::create(&self.corpus_path);
    let pool = self.open_pool();
    subset_corpus(&corpus, &out, targets, &pool)?;

    out.shutdown()
  }

  fn open_pool(&self) -> ThreadPool {
    let n = self.n_jobs.unwrap_or(2);
    eprintln!("using {} threads", n);
    ThreadPool::new(n)
  }
}
