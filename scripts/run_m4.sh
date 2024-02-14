#!/bin/bash0,
a=$1
q=$2
p=$3
aggFactor=$4
w=$5
h=$6
type=$7

tables=("manufacturing_exp" "intel_lab_exp" "soccer_exp")
modes=("m4")

for table in "${tables[@]}"
do
    for mode in "${modes[@]}"
    do
        out="m4-$q"
        java -jar target/experiments.jar -c timeQueries -measureChange 0 -seqCount 100 -type "$type" -mode "$mode" -measures 0,2 -timeCol datetime -zoomFactor 2 -viewport 1000,600 -runs 3 -out "$out" -minShift 0.1 -maxShift 0.5 -schema more -table "$table" -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a "$a" -q "$q" -p "$p" -agg "$aggFactor"
    done
done