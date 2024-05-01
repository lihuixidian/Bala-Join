#!/bin/bash
proto_name=${1}
dir=${1%.*}
grpc_cpp_plugin_path="/root/.local/bin/grpc_cpp_plugin"

rm -rf ../${dir}
mkdir ../${dir}

echo "-----------------excute command--------------------"
echo "protoc --cpp_out=../${dir} ${proto_name}"
echo "protoc --grpc_out=../${dir} --plugin=protoc-gen-grpc=${grpc_cpp_plugin_path} ${proto_name}"
echo "---------------------------------------------------"
protoc --cpp_out=../${dir} ${proto_name}
protoc --grpc_out=../${dir} --plugin=protoc-gen-grpc=${grpc_cpp_plugin_path} ${proto_name}

# Create CMakeLists.txt
cd ../${dir}
project_name=${dir}
echo "cmake_minimum_required(VERSION 3.10)

project(${project_name})
set(proto_name \"${project_name}\")

set(proto_srcs \"\${CMAKE_CURRENT_SOURCE_DIR}/\${proto_name}.pb.cc\")
set(proto_hdrs \"\${CMAKE_CURRENT_SOURCE_DIR}/\${proto_name}.pb.h\")
set(grpc_srcs \"\${CMAKE_CURRENT_SOURCE_DIR}/\${proto_name}.grpc.pb.cc\")
set(grpc_hdrs \"\${CMAKE_CURRENT_SOURCE_DIR}/\${proto_name}.grpc.pb.h\")

# Package generated code files
add_library(\${proto_name}_service
  \${grpc_srcs}
  \${grpc_hdrs}
  \${proto_srcs}
  \${proto_hdrs})
target_link_libraries(\${proto_name}_service
  \${_REFLECTION}
  \${_GRPC_GRPCPP}
  \${_PROTOBUF_LIBPROTOBUF})" > CMakeLists.txt