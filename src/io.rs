/// Common IO routines.

use std::io::prelude::*;
use std::path::{Path};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, BufReader, Result};

use flate2::bufread::MultiGzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use indicatif::{ProgressBar, ProgressStyle};

const PB_STYLE: &str = "{prefix:15}: {bar:25} {percent}% {bytes}/{total_bytes} (eta {eta})";

/// Initialize a new default progress bar.
pub fn make_progress() -> ProgressBar {
  let style = ProgressStyle::default_bar().template(PB_STYLE);
  let pb = ProgressBar::new(1024);
  pb.set_style(style);
  pb
}

/// Open a GZIP-compressed output file.
pub fn open_gzout<P: AsRef<Path>>(path: P) -> Result<Box<dyn Write>> {
  let output = open_gzout_unbuffered(path)?;
  let output = BufWriter::new(output);
  Ok(Box::new(output))
}

/// Open a GZIP-compressed output file without buffering.
pub fn open_gzout_unbuffered<P: AsRef<Path>>(path: P) -> Result<Box<dyn Write>> {
  let output = OpenOptions::new().write(true).truncate(true).create(true).open(path)?;
  // buffer twice for efficiency
  let output = BufWriter::new(output);
  let output = GzEncoder::new(output, Compression::best());
  Ok(Box::new(output))
}

/// Open a GZIP-compressed input file with buffering and progress.
pub fn open_gzin<P: AsRef<Path>>(path: P, pb: &ProgressBar) -> Result<Box<dyn BufRead>> {
  let path = path.as_ref();
  let src = File::open(path)?;
  let name = path.file_name().unwrap().to_string_lossy();

  pb.set_length(src.metadata()?.len());
  pb.set_prefix(&name);
  let src = pb.wrap_read(src);
  let src = BufReader::new(src);
  let src = MultiGzDecoder::new(src);
  let src = BufReader::new(src);

  Ok(Box::new(src))
}
