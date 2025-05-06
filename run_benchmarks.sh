#! /usr/bin/bash

FILENAME=benchmark_results.txt

rm $FILENAME

for k in 22 24 28; do
  for t in 7 4 3; do
    for run in 1 2 3; do
      ./chia_plotter -k "$k" -t "$t" -f data -m 4096 | tee -a "$FILENAME"
    done
  done
done