#!/bin/sh

while getopts 'I:R:G:' c
do
  case $c in
    G) GROUPFILE_CSV=$OPTARG ;;
    R) RELFILE_JSON=$OPTARG ;;
    I) RUNFILE_JSON=$OPTARG ;;
  esac
done


./trec/json2qrels.py -G $GROUPFILE_CSV -R $RELFILE_JSON -c > qrels.tsv
./trec/json2runfile.py -I $RUNFILE_JSON > runfile.tsv

./expeval.py -I runfile.tsv -R qrels.tsv -G -C