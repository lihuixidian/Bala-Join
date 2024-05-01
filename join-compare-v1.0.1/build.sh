#!/bin/bash
rm -rf build
mkdir build
rm -rf lib
mkdir lib
rm -rf bin
mkdir bin

cd build
cmake -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR ../src
make -j4