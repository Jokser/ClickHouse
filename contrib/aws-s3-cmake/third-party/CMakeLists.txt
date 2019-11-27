cmake_minimum_required(VERSION 3.1)

include(ExternalProject)

set(CMAKE_INSTALL_PREFIX, "${CMAKE_BINARY_DIR}/aws-common2")

set(AWS_DEPS_INSTALL_DIR "${CMAKE_BINARY_DIR}/aws-common" CACHE PATH "Dependencies install directory.")
set(AWS_DEPS_BUILD_DIR "${CMAKE_BINARY_DIR}/build" CACHE PATH "Dependencies build directory.")
set(AWS_DEPS_DOWNLOAD_DIR "${AWS_DEPS_BUILD_DIR}/downloads" CACHE PATH "Dependencies download directory.")

set(AWS_C_COMMON_URL "https://github.com/awslabs/aws-c-common.git")
set(AWS_C_COMMON_TAG "v0.4.15")
include(cmake/BuildAwsCCommon.cmake)

set(AWS_CHECKSUMS_URL "https://github.com/awslabs/aws-checksums.git")
set(AWS_CHECKSUMS_TAG "v0.1.5")
include(cmake/BuildAwsChecksums.cmake)

set(AWS_EVENT_STREAM_URL "https://github.com/awslabs/aws-c-event-stream.git")
set(AWS_EVENT_STREAM_TAG "v0.1.4")
include(cmake/BuildAwsEventStream.cmake)
