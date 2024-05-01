#!/bin/bash

# Compress the current directory
cp cluster.txt ../
cd ../
tar -cvzf /tmp/tmp.tgz join-compare-v1.0.1
# Copy the compressed package to the cluster
parallel-scp -h cluster.txt /tmp/tmp.tgz ~/
# Decompress the file
parallel-ssh -h cluster.txt -i "tar -xzvf ~/tmp.tgz && rm -f ~/tmp.tgz"
rm -f /tmp/tmp.tgz
rm -f cluster.txt
