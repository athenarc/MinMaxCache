#!/bin/bash

#a=$1
#q=$2
#p=$3
#aggFactor=$4
#reductionRatio=$5
#type=$6

# # Agg Ratio
# sh scripts/run.sh 0.95 0.1 1 2 1000 600 influx
# sh scripts/run.sh 0.95 0.1 1 4 1000 600 influx
# sh scripts/run.sh 0.95 0.1 1 8 1000 600 influx
# sh scripts/run.sh 0.95 0.1 1 16 1000 600 influx
# sh scripts/run.sh 0.95 0.1 1 32 1000 600 influx

## Error Bound
# sh scripts/run.sh 0.9 0.1 1 4 1000 600 influx
# sh scripts/run.sh 0.95 0.1 1 4 1000 600 influx
# sh scripts/run.sh 0.99 0.1 1 4 1000 600 influx

# Start Query  Selectivity
# sh scripts/run.sh 0.95 0.01 1 4 1000 600 influx
# sh scripts/run.sh 0.95 0.05 1 4 1000 600 influx
# sh scripts/run.sh 0.95 0.1 1 4 1000 600 influx
# sh scripts/run.sh 0.95 0.5 1 4 1000 600 influx

## No prefetching
#sh scripts/run.sh 0.95 0.1 0 4 1000 600 influx
#sh scripts/run.sh 0.95 0.1 0.1 4 1000 600 influx
#sh scripts/run.sh 0.95 0.1 0.2 4 1000 600 influx
#sh scripts/run.sh 0.95 0.1 0.5 4 1000 600 influx
#sh scripts/run.sh 0.95 0.1 1 4 1000 600 influx
#sh scripts/run.sh 0.95 0.1 1.4 4 1000 600 influx
#
sh scripts/run.sh 0.95 0.1 1 4 250 150 influx
sh scripts/run.sh 0.95 0.1 1 4 500 300 influx
sh scripts/run.sh 0.95 0.1 1 4 1000 600 influx
sh scripts/run.sh 0.95 0.1 1 4 2000 1200 influx
sh scripts/run.sh 0.95 0.1 1 4 4000 2400 influx

