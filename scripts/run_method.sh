#!/bin/bash0,
a=$1
q=$2
p=$3
aggFactor=$4
w=$5
h=$6
type=$7

tables=("manufacturing_exp")
modes=("m4")

for table in "${tables[@]}"
do
    for mode in "${modes[@]}"
    do
        out="method-$q-$a"
        java -jar target/experiments.jar -c timeQueries -measureChange 0 -seqCount 50 -type "$type" -mode "$mode" -measures 2 -timeCol timestamp -idCol id -valueCol value -zoomFactor 2 -viewport "$w","$h" -runs 1 -out "$out" -minShift 0.1 -maxShift 0.5 -schema more -table "$table" -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a "$a" -q "$q" -p "$p" -agg "$aggFactor" -queries queries.txt
    done
done