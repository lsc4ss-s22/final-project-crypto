# __The Evolution of Spillovers Between Cryptocurrencies and Conventional Currencies: The Complexity and Dynamism of Cross-Market Interaction__

This is the GitHub repository for the final project of MACS 30123 Large Scale Computing.

__Author:__ Shiyang Lai, Peihan Gao, Juno Wu, Coco Yu

The codes were written in Python 3.9.7 and RStudio, and all of its dependencies can be installed by running the following in the terminal (with the `requirements.txt` file included in this repository):
```
pip install -r requirements.txt
```

## Project Description
The current project aims at exploring the spillovers between cryptocurrencies and conventional currencies. We pooled historical historical data from XX mainstream conventional currencies and cryptocurrencies from XX to XX, and employed a compute-intensive framework incorporating both econometric measures and up-to-date neural architectures for data analysis. Raw data are available both in the `Raw data` folder in this github repository (https://github.com/lsc4ss-s22/final-project-crypto/tree/main/Raw%20data) and in a S3 bucket (https://crypto-conven-training.s3.us-west-2.amazonaws.com). To speed up calculations, we utilized large-scale parallel processing including MPI, PySpark, and GPU computing.

## Data Processing and Analysis
### 1. Spark-based Data Preprocessing Pipeline
> Read currency data and calculate returns and volatility of currencies.

**Codes available at:** https://github.com/lsc4ss-s22/final-project-crypto/blob/main/Python%20scripts/data%20preprocessing.py

**Speedup (PySpark 2.4):** https://github.com/lsc4ss-s22/final-project-crypto/blob/main/Notebooks/pyspark_graphframe.ipynb

### 2. Paralleled Economic Spillovers Computation with R
> Calculate spillovers and output network graph

**Codes availabe at:** https://github.com/lsc4ss-s22/final-project-crypto/blob/main/R%20Scripts/parallel.R

**Visualizaton (network graph reflecting connectedness and spillovers):** 

**Speedup (Rmpi Package for R):**\
Online documentation: https://cran.r-project.org/web/packages/Rmpi/index.html \
Rmpi installation for Mac OS X: http://fisher.stats.uwo.ca/faculty/yu/Rmpi/mac_os_x.htm \
Rmpi installation for Windows: http://fisher.stats.uwo.ca/faculty/yu/Rmpi/windows.htm \
Rmpi installation for Linux: http://fisher.stats.uwo.ca/faculty/yu/Rmpi/install.htm

### 3. Data Storage with S3 and Dynamo DB
> Output files in the previous two steps were uploaded to S3 bucket `crypto-conven-training (us-west-2)` and Dynamo DB `table=return_prediction`

**Codes available at:** https://github.com/lsc4ss-s22/final-project-crypto/blob/main/Notebooks/to_dynamodb.ipynb

### 4. Deep Learning Model Training using AWS EC2 Instances and Midway GPU Nodes

## __To-do List__
- [x] Spark-based Data Preprocessing Pipeline
- [x] Paralleled Economic Spillovers Computation with R
- [x] Data Storage with S3 and DynamoDB
- [x] Deep Learning Model Training using AWS EC2 Instances and Midway GPU Nodes 
- [x] Scalable Hyperparameter Tuning with _Ray_
- [x] Spillovers Network Analysis with Pyspark Graphframe
- [x] Visualization
