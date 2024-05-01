#!/bin/bash

sh build.sh

# Copy the compressed package to the cluster
cp bin/main /tmp
sh clusterRun.sh "rm -f ~/join-compare-v1.0.1/bin/main"
parallel-scp -h cluster.txt /tmp/main ~/join-compare-v1.0.1/bin/
rm -f /tmp/main
