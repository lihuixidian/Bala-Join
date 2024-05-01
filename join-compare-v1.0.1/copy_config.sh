#!/bin/bash

# Copy the configuration file to the cluster
parallel-scp -h cluster.txt config/join-compare.ini ~/join-compare-v1.0.1/config/
