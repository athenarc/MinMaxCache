#!/bin/bash

a=$1
q=$2
p=$3
aggFactor=$4
w=$5
h=$6
type=$7

tables=("manufacturing_exp")
modes=("minMax")

for table in "${tables[@]}"
do
    for mode in "${modes[@]}"
    do
       out="output_q=${q}_a=${a}_p=${p}_a_ratio=${aggFactor}_w=${w}_h=${h}"
        # out="output_scalability_q=${q}_a=${a}_p=${p}_a_ratio=${aggFactor}_r_ratio=${reductionRatio}"
        # out="m4-0.1"
        java -jar target/experiments.jar -c timeQueries -seqCount 50 -measureChange 0 -type "$type" -mode "$mode" -measures 2 -timeCol epoch -valueCol value -idCol id -zoomFactor 2 -viewport "$w","$h" -runs 3 -out "$out" -minShift 0.1 -maxShift 0.5 -schema more -table "$table" -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a "$a" -q "$q" -p "$p" -agg "$aggFactor"
    done
done