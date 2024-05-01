cmake_minimum_required(VERSION 3.10)

project(stream)
set(proto_name "stream")

set(proto_srcs "${CMAKE_CURRENT_SOURCE_DIR}/${proto_name}.pb.cc")
set(proto_hdrs "${CMAKE_CURRENT_SOURCE_DIR}/${proto_name}.pb.h")
set(grpc_srcs "${CMAKE_CURRENT_SOURCE_DIR}/${proto_name}.grpc.pb.cc")
set(grpc_hdrs "${CMAKE_CURRENT_SOURCE_DIR}/${proto_name}.grpc.pb.h")

# 生成代码文件打包
add_library(${proto_name}_service
  ${grpc_srcs}
  ${grpc_hdrs}
  ${proto_srcs}
  ${proto_hdrs})
target_link_libraries(${proto_name}_service
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})
