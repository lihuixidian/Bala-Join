cmake_minimum_required(VERSION 3.10)

project(grpc_demo)

include(../cmake/common.cmake)

# 设置可执行文件生成路径
set(EXECUTABLE_OUTPUT_PATH ../bin)
# 设置可执行文件生成路径
set(LIBRARY_OUTPUT_PATH ../lib)

include_directories(${PROJECT_SOURCE_DIR}/)

# 添加服务子项目
add_subdirectory(services/stream)

# 添加所有源文件
aux_source_directory(./comm DIR_SRCS)
aux_source_directory(./base DIR_SRCS)
aux_source_directory(./core DIR_SRCS)
aux_source_directory(./thirdparty DIR_SRCS)
aux_source_directory(./serviceImpl DIR_SRCS)

# client
add_executable(client "stream_client.cc" ${DIR_SRCS})
target_link_libraries(client
  stream_service
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

# server
add_executable(server "stream_server.cc" ${DIR_SRCS})
target_link_libraries(server
  stream_service
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

add_executable(main "main.cpp" ${DIR_SRCS})
target_link_libraries(main
  stream_service
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})
