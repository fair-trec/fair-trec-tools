#!/usr/bin/env python3

import argparse
import json
import csv
import os

def main():
    parser = argparse.ArgumentParser(description='convert fair ranking runfile json to trec runfile')
    parser.add_argument('-I', dest='runfile', required=True)
    parser.add_argument('-r', dest='run_id')

    args = parser.parse_args()
    runfile = args.runfile
    if not(args.run_id != None):
        run_id = os.path.basename(runfile)
    else:
        run_id = args.run_id
    qid_cnts = {}
    with open(runfile,"r") as fp:
        for line in fp:
            data = json.loads(line.strip())
            qid = "%d"%data["qid"]
            if not (qid in qid_cnts):
                qid_cnts[qid] = 0
            else:
                qid_cnts[qid] = qid_cnts[qid] + 1
            qid_idx = qid_cnts[qid]
            rank = 1
            for did in data["ranking"]:
                row = []
                row.append(qid)
                row.append("Q%d"%qid_idx)
                row.append(did)
                row.append("%d"%rank)
                row.append("%f"%(1.0/rank))
                row.append(run_id)
                print("\t".join(row))
                rank = rank + 1
        
if __name__ == '__main__':
    main()
