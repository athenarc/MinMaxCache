
# Visualization-aware Timeseries Min-Max Caching with Error Bound Guarantees

## Supplemental Material
Supplemental material detailing the algorithms for error-bound calculation and query evaluation over MinMaxCache, as well as a detailed presentation of the user study conducted, can be found [here](https://github.com/athenarc/MinMaxCache/blob/main/paper_supplementary_material.pdf).
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
java -jar target/experiments.jar -path <path_to_csv> -c initialize -type <influx, postgres> -out output -schema <schema> -table <table_name> -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"
```
To execute a sequence of queries, e.g. using a table, run the following:

```
java -jar target/experiments.jar -c timeQueries -seqCount 50 -measureChange 0 -type <influx, postgres> -mode <minMax> -measures <measure_ids> -timeCol <timeCol (if postgres)>
-valueCol <valueCol (if postgres)> -idCol <idCol (if postgres)> -zoomFactor 2 -viewport <width,height> -runs 1 -out <output_folder_path> -minShift 0.1 -maxShift 0.5 -schema <schema or bucket> -table <tableName> -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.95 -q 0.1 -p 1 -agg 4 
```

### Parameters:

-c *The command to run, <timeQueries, initialize>*

-seqCount *No. of queries*

-type *Database, <influx, postgres>*

-mode *Algorithm to run <minMax, m4, raw>

-measures *Measure ids, e.g 1,2,3*

-timeCol *Name of time column (for postgres)*

-valueCol *Name of value column (for postgres)*

-idCol *Name of id column (for postgres)*

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

(-queries) *A path to a csv file with predefined epoch-based queries. First column is start epoch and the second end epoch (e.g queries.txt file in the repository). *

