# Fair TREC Tools

Public tools for working with the Fair TREC data.

## Environment & Compilation

The provided Conda environment spec will install all required dependencies, except for the
[AWS CLI tools][aws-cli] needed for downloading the Open Corpus:

    conda env create -f environment.yml
    conda activate fairtrec

High-throughput data processing tools, such as the subsetter, are implemented in Rust (installed
in the Conda environment); to build, run:

    cargo build --release

[aws-cli]: https://aws.amazon.com/cli/

## Downloading Data

You need two sets of data:

1.  The released data files from the Fair TREC web site, stored in `data/ai2-trec-release`

2.  The [Open Corpus][OC], downloaded to `data/corpus` with:

        aws s3 cp --no-sign-request --recursive s3://ai2-s2-research-public/open-corpus/2020-05-27/ data/corpus

[OC]: http://s2-public-api-prod.us-west-2.elasticbeanstalk.com/corpus/download/


## Subsetting the Corpus

To re-generate the OpenCorpus subet containing all files in the paper metadata file, run:

    ./target/release/subset-corpus -M data/ai2-trec-release/paper_metadata.csv \
        -o data/corpus-subset-for-meta.gz data/corpus

To generate a subset based on the candidate sets from query records, run:

    ./target/release/subset-corpus -Q data/TREC-Competition-training-sample.json \
        -o data/corpus-subset-for-queries.jsonl.gz data/corpus

The subset command also produces metadata CSV alongside the compressed JSON output.

The `--help` option works and will produce usage help.
