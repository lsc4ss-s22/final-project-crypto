#!/bin/sh
#SBATCH --job-name=pytorch
#SBATCH --ntasks=1
#SBATCH --mem-per-cpu=10G    
#SBATCH --output=example.out
#SBATCH --error=gpu.err     # error file
#SBATCH --partition=gpu2    # GPU2 partition
#SBATCH --nodes=1           # 1 GPU node
#SBATCH --ntasks=1          # 1 CPU core to drive GPU
#SBATCH --gres=gpu:1        # Request 1 GPU
#SBATCH --account=macs30123 # specify the account

sudo yum update

aws s3 cp s3://crypto-conven-training/requirement.txt .

pip3 install -r requirement.txt

aws s3 cp s3://crypto-conven-training/python python/ --recursive

aws s3 cp s3://crypto-conven-training/preprocessed data/ --recursive

cd python

python3 main.py USDGBP