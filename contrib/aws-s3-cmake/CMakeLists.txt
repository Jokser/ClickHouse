SET(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-s3)
SET(CORE_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-core)

file(GLOB AWS_S3_HEADERS
        "${LIBRARY_DIR}/include/aws/s3/*.h"
        )

file(GLOB AWS_S3_MODEL_HEADERS
        "${LIBRARY_DIR}/include/aws/s3/model/*.h"
        )

file(GLOB AWS_S3_CORE_HEADERS
        "${CORE_LIBRARY_DIR}/include/aws/core/*.h"
        "${CORE_LIBRARY_DIR}/include/aws/core/utils/*.h"
        "${CORE_LIBRARY_DIR}/include/aws/core/http/*.h"
        "${CORE_LIBRARY_DIR}/include/aws/core/client/*.h"
        "${CORE_LIBRARY_DIR}/include/aws/core/auth/*.h"
        )

file(GLOB AWS_S3_CORE_SOURCES
        "${CORE_LIBRARY_DIR}/source/core/*.h"
        "${CORE_LIBRARY_DIR}/source/core/utils/*.h"
        "${CORE_LIBRARY_DIR}/source/core/http/*.h"
        "${CORE_LIBRARY_DIR}/source/core/client/*.h"
        "${CORE_LIBRARY_DIR}/source/core/auth/*.h"
        )

file(GLOB AWS_S3_SOURCE
        "${LIBRARY_DIR}/source/*.cpp"
        )

file(GLOB AWS_S3_MODEL_SOURCE
        "${LIBRARY_DIR}/source/model/*.cpp"
        )

file(GLOB S3_UNIFIED_HEADERS
        ${AWS_S3_HEADERS}
        ${AWS_S3_MODEL_HEADERS}
        ${AWS_S3_CORE_HEADERS}
        )

file(GLOB S3_UNITY_SRC
        ${AWS_S3_SOURCE}
        ${AWS_S3_MODEL_SOURCE}
        ${AWS_S3_CORE_SOURCES}
        )

file(GLOB S3_SRC
        ${S3_UNIFIED_HEADERS}
        ${S3_UNITY_SRC}
        )

set(S3_INCLUDES
        "${CMAKE_CURRENT_SOURCE_DIR}/include/"
        ${AWS_S3_CORE_HEADERS}
        )

add_library(aws_s3 ${S3_SRC})
target_include_directories(aws_s3 PUBLIC S3_INCLUDES)
