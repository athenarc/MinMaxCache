
# Visualization-aware Timeseries Min-Max Caching with Error Bound Guarantees

## Supplemental Material
Supplemental material detailing the theorems and proofs used in the paper can be found [here](https://imisathena-my.sharepoint.com/:b:/g/personal/bstam_athenarc_gr/EZLMpCbHnvBAr451eO73T78BC93AOf0OPwmwxltP6H2W4Q?e=wMDCSG).
## Datasets
Data used for the experiments can be found [here](https://imisathena-my.sharepoint.com/:f:/g/personal/bstam_athenarc_gr/EqNFfVTRJ_9KresHs-QGyQ8BYJZVOQNty_mRCIwpru7s-Q?e=PoAxgl).

In this folder, there are 2 sub-folders. One for the real datasets used in the experiments and one for the synthetic. Each contains a notebook, named expand_data.ipynb and create_synth.ipynb respectively.

Required libraries to run both notebooks are: numpy, pandas and datetime.

By running the expand_data.ipynb, the original datasets will be expanded 50 times, and 3 datasets with the same name and the suffix "_exp" will be created.
Running create_synth.ipynb, will create 11 synthetic timeseries datasets generated from random walks, with the names synthetic_{1m-1b}.csv.

## Running Instructions

First, build the JAR file:

```
mvn clean install
```

To initialize a dataset in a .csv file, run the following:
```
java -jar target/experiments.jar -path <path_to_csv> -c initialize -type <influx, postgres> -timeCol <time_column> 
-out output -schema <schema> -table <table_name> -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"
```
To execute a sequence of queries, e.g. using a table, run the following:

```
java -jar target/experiments.jar -c timeQueries -seqCount 50 -type <influx, postgres> -mode <ttiMinMax> -measures <measure_ids> -zoomFactor 2 -viewport <width,height> -runs 5 -out "$out" -minShift 0.001 -maxShift 1 -schema more -table "$table" -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.95 -q 0.1 -p 1 -agg 4 -reduction 4
```

### Parameters:

-c *The command to run, <timeQueries, initialize>*

-seqCount *No. of queries*

-type *Database, <influx, postgres>*


-mode *Algorithm to run <ttiMinMax, m4, raw>

-measures *Measure ids, e.g 1,2,3*

-timeCol *Name of time column (for initialization)*

-out *Output Folder*

-schema *Schema on the DB (On Influx it defines the bucket)

-table *Table Name*

-zoomFactor *Factor by which to zoom in and out%*

-viewPort *Width, height of the viewport of visualization*

-runs *No. of times to run the experiment*

-minShift *Minimum pan shift*

-maxShift *Maximum pan shift*

-timeFormat *Time format of the Time Column*

-a *Accuracy Threshold*

-q *Query Selectivity*

-p *Prefetching Toggle*

-agg *Initial Aggregation Factor*

-reduction *Data Reduction Factor*


