SET(AWS_S3_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-s3)
SET(AWS_CORE_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-core)
SET(AWS_CHECKSUMS_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/aws-checksums)
SET(AWS_COMMON_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/aws-c-common)
SET(AWS_EVENT_STREAM_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/aws-c-event-stream)

file(GLOB AWS_S3_HEADERS
        "${AWS_S3_LIBRARY_DIR}/include/aws/s3/*.h"
        )

file(GLOB AWS_S3_MODEL_HEADERS
        "${AWS_S3_LIBRARY_DIR}/include/aws/s3/model/*.h"
        )

file(GLOB AWS_CORE_HEADERS
        "${AWS_CORE_LIBRARY_DIR}/include/aws/core/*.h"
        "${AWS_CORE_LIBRARY_DIR}/include/aws/core/utils/*.h"
        "${AWS_CORE_LIBRARY_DIR}/include/aws/core/http/*.h"
        "${AWS_CORE_LIBRARY_DIR}/include/aws/core/client/*.h"
        "${AWS_CORE_LIBRARY_DIR}/include/aws/core/auth/*.h"
        )

file(GLOB AWS_CORE_SOURCES
        "${AWS_CORE_LIBRARY_DIR}/source/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/auth/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/client/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/http/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/http/standard/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/config/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/external/cjson/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/external/tinyxml2/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/internal/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/monitoring/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/net/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/linux-shared/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/platform/linux-shared/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/utils/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/utils/base64/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/utils/event/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/utils/crypto/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/utils/crypto/openssl/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/utils/crypto/factory/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/utils/json/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/utils/logging/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/utils/memory/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/utils/memory/stl/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/utils/stream/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/utils/threading/*.cpp"
        "${AWS_CORE_LIBRARY_DIR}/source/utils/xml/*.cpp"
        )

file(GLOB AWS_EVENT_STREAM_HEADERS
        "${AWS_EVENT_STREAM_LIBRARY_DIR}/include/event-stream/*.h"
        )

file(GLOB AWS_EVENT_STREAM_SOURCES
        "${AWS_EVENT_STREAM_LIBRARY_DIR}/source/*.c"
        )

file(GLOB AWS_COMMON_SOURCES
        "${AWS_COMMON_LIBRARY_DIR}/source/*.c"
        "${AWS_COMMON_LIBRARY_DIR}/source/posix/*.c"
        )

file(GLOB AWS_CHECKSUMS_SOURCES
        "${AWS_CHECKSUMS_LIBRARY_DIR}/source/*.c"
#        "${AWS_CHECKSUMS_LIBRARY_DIR}/source/intel/*.c"
#        "${AWS_CHECKSUMS_LIBRARY_DIR}/source/arm/*.c"
#        "${AWS_CHECKSUMS_LIBRARY_DIR}/source/visualc/*.c"
        )

file(GLOB AWS_S3_SOURCES
        "${AWS_S3_LIBRARY_DIR}/source/*.cpp"
        )

file(GLOB AWS_S3_MODEL_SOURCES
        "${AWS_S3_LIBRARY_DIR}/source/model/*.cpp"
        )

file(GLOB S3_UNIFIED_HEADERS
        ${AWS_EVENT_STREAM_HEADERS}
        ${AWS_S3_HEADERS}
        ${AWS_S3_MODEL_HEADERS}
        ${AWS_CORE_HEADERS}
        )

file(GLOB S3_UNIFIED_SRC
        ${AWS_EVENT_STREAM_SOURCES}
        ${AWS_COMMON_SOURCES}
        ${AWS_CHECKSUMS_SOURCES}
        ${AWS_S3_SOURCES}
        ${AWS_S3_MODEL_SOURCES}
        ${AWS_CORE_SOURCES}
        )

file(GLOB S3_SRC
        ${S3_UNIFIED_HEADERS}
        ${S3_UNIFIED_SRC}
        )

set(S3_INCLUDES
        "${CMAKE_CURRENT_SOURCE_DIR}/include/"
        "${AWS_COMMON_LIBRARY_DIR}/include/"
        "${AWS_CHECKSUMS_LIBRARY_DIR}/include/"
        "${AWS_EVENT_STREAM_LIBRARY_DIR}/include/"
        "${AWS_S3_LIBRARY_DIR}/include/"
        "${AWS_CORE_LIBRARY_DIR}/include/"
        )

add_library(aws_s3 ${S3_SRC})

target_compile_definitions(aws_s3 PUBLIC "AWS_SDK_VERSION_MAJOR=1")
target_compile_definitions(aws_s3 PUBLIC "AWS_SDK_VERSION_MINOR=7")
target_compile_definitions(aws_s3 PUBLIC "AWS_SDK_VERSION_PATCH=231")

target_compile_options(aws_s3 PUBLIC -fPIC)

target_include_directories(aws_s3 PRIVATE ${S3_INCLUDES})
