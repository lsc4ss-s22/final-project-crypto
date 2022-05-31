#!/bin/sh

sudo yum update

aws s3 cp s3://crypto-conven-training/requirement.txt .

pip3 install -r requirement.txt

aws s3 cp s3://crypto-conven-training/python python/ --recursive

aws s3 cp s3://crypto-conven-training/preprocessed data/ --recursive

cd python

python main.py USDBTC
