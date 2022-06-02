# __The Evolution of Spillovers Between Cryptocurrencies and Conventional Currencies: The Complexity and Dynamism of Cross-Market Interaction__

This is the GitHub repository for the final project of MACS 30123 Large Scale Computing.

__Author:__ Shiyang Lai, Peihan Gao, Juno Wu, Coco Yu

The codes were written in Python 3.9.7 and R, and all of its dependencies can be installed by running the following in the terminal (with the `requirements.txt` file included in this repository):
```
$ pip install -r requirements.txt
```

## Project Description
As the cryptocurrency market becomes more developed and popular, more information is needed on how it connects with the traditional currency market. Will the cryptocurrency market's fluctuation has a big effect on traditional currency market that may cause it to fluctuate as well, or will it be the other way around, or both? The current project aims at exploring the spillovers between cryptocurrencies and conventional currencies. We pooled historical data from 155 mainstream conventional currencies and cryptocurrencies from Sep 1 2015 to May 1 2022, and employed a compute-intensive framework incorporating both econometric measures and up-to-date neural architectures for data analysis. Raw data are available both in the `Raw data` folder in this github repository (https://github.com/lsc4ss-s22/final-project-crypto/tree/main/Raw%20data) and in a S3 bucket (https://crypto-conven-training.s3.us-west-2.amazonaws.com). To speed up calculations, we utilized large-scale parallel processing including MPI, PySpark, and GPU computing. We believe that this project will provide more in-depth exploration of the diversity, dynamics, and other complex characteristics of their interaction.Also, it brings significant implications to investors and policymakers about potential risks and opportunities. 

![image](https://user-images.githubusercontent.com/91500767/171509247-9f34c236-a909-4135-9a3b-3bdde478e16b.png)


## Data Processing and Analysis
### 1. Spark-based Data Preprocessing Pipeline
> Read currency data and calculate returns and volatility of currencies.

**Codes available at:** <a href="https://github.com/lsc4ss-s22/final-project-crypto/blob/main/Python%20scripts/data%20preprocessing.py">data preprocessing.py</a>

<!-- **Speedup (PySpark 2.4)??????:** <a href="https://github.com/lsc4ss-s22/final-project-crypto/blob/main/Notebooks/pyspark_graphframe.ipynb"> pyspark_graphframe.ipynb</a> -->

### 2. Paralleled Economic Spillovers Computation with R
> Do VAR lag fitting, calculate spillovers, and output network graph.
> After the implementation of Rmpi(parallel processing locally with work distributed to 3 cores, the running time is down to xx, in comparison to several hours when serial processing. 

**Codes availabe at:** <a href="https://github.com/lsc4ss-s22/final-project-crypto/blob/main/R%20Scripts/parallel.R"> parallel.R</a>

Spillover networks under short-, medium-, and long-frequency bands

<img src="Images/short-frequency.png" width="260"/><img src="Images/medium-frequency.png" width="260"/><img src="Images/long-frequency.png" width="260"/>


**Speedup (Rmpi Package for R):**\
Online documentation: https://cran.r-project.org/web/packages/Rmpi/index.html \
Rmpi installation for Mac OS X: http://fisher.stats.uwo.ca/faculty/yu/Rmpi/mac_os_x.htm \
Rmpi installation for Windows: http://fisher.stats.uwo.ca/faculty/yu/Rmpi/windows.htm \
Rmpi installation for Linux: http://fisher.stats.uwo.ca/faculty/yu/Rmpi/install.htm

### 3. Data Storage with S3 and Dynamo DB
> Output files in the previous two steps were uploaded to S3 bucket `crypto-conven-training (us-west-2)` and Dynamo DB `table=return_prediction`

**Codes available at:** <a href="https://github.com/lsc4ss-s22/final-project-crypto/blob/main/Notebooks/to_dynamodb.ipynb"> to_dynamodb.ipynb</a>

Snapshot of S3 Bucket
![image](/Snapshots/AWS_S3_Bucket.png)
Snapshot of DynamoDB
![image](/Snapshots/AWS_DynamoDB.jpeg)

### 4. Deep Learning Model Training using AWS EC2 Instances and Midway GPU Nodes
> Use deep learning models to predict prices of cryptocurrencies and conventional currencies

**Helper packages available at:**
- <a href="https://github.com/lsc4ss-s22/final-project-crypto/blob/main/Python%20scripts/utils.py">`utils.py`</a> 
- Model Training: Scikit-learn <a href="https://github.com/lsc4ss-s22/final-project-crypto/blob/main/Python%20scripts/sklearn_training.py">`sklearn_training.py`</a>; PyTorch <a href="https://github.com/lsc4ss-s22/final-project-crypto/blob/main/Python%20scripts/torch_training.py">`torch_training.py`</a> 
- Model Evaluation: <a href="https://github.com/lsc4ss-s22/final-project-crypto/blob/main/Python%20scripts/evaluation.py"> `evaluation.py`</a>

**Run the following codes using AWS EC2 CPU training with <a href="https://github.com/lsc4ss-s22/final-project-crypto/blob/main/job.sh">job.sh</a>:**
``` shell
$ bash job.sh
```
Snapshot of deploying models on an AWS EC2 t2.large instance
![image](Snapshots/AWS_EC2_CPU_Training.png)

**Run the following codes using Midway2 GPU training with <a href="https://github.com/lsc4ss-s22/final-project-crypto/blob/main/job.sbatch">job.sbatch</a>:**
``` shell
$ sbatch job.sbatch
```
Snapshot of deploying models on an GPU node in gpu2 cluster
![image](Snapshots/Midway_GPU_Training2.png)
We ran six models (i.e., BiLSTMs, ResNet, gMLP, TST, MLSTM-FCN, XCM) on six currencies (i.e., EUR, GBP, AUD, BTC, ETH, DOGE). It took about 2-hour to use one GPU on midway2 to finish the training for one currency on the six models.

### 5. Scalabel Hyperparameter Tuning with _Ray_
**Source codes available at:** <a href="https://github.com/lsc4ss-s22/final-project-crypto/blob/main/Python%20scripts/tuning.py">tuning.py</a>

Snapshot of the tuning result of BiLSTMs
![image](Snapshots/Ray_Tunning.png)

### 6. Spillovers Network Analysis with Pyspark Graphframe
**Codes available at:** <a href="https://github.com/lsc4ss-s22/final-project-crypto/blob/main/Notebooks/graphframe_analysis.ipynb">graphframe_analysis.ipynb</a>

### 7. Visualization
> Plot predictability comparison (with cross-market information and without) for the six models

**Codes available at:** <a href="https://github.com/lsc4ss-s22/final-project-crypto/blob/main/Notebooks/visualization.ipynb">visualization.ipynb</a>

![plot](https://github.com/lsc4ss-s22/final-project-crypto/blob/main/Images/lm_results.svg)

## Contributions
Data gathering and preprocessing – Shiyang Lai & Peihan Gao

VARX-L model-based currency spillovers calculation – Juno Wu & Coco Yu

Prediction models training, running and evaluation – Shiyang Lai & Peihan Gao

README file creation – Juno Wu & Coco Yu

Presentation visualization – Juno Wu & Coco Yu

Presentation recording – Shiyang Lai, Peihan Gao, Coco Yu, Juno Wu

## License
Copyright (c) 2022 lsc4ss-s22-group-project

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
