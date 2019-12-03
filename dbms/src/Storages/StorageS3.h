#pragma once

#include <Storages/IStorage.h>
#include <Poco/URI.h>
#include <common/logger_useful.h>
#include <ext/shared_ptr_helper.h>
#include <aws/s3/S3Client.h>
#include <aws/core/auth/AWSCredentialsProvider.h>

namespace DB
{
// Structure to describe S3 endpoint.
struct S3Endpoint {
    // S3 Endpoint URL.
    String endpoint_url;
    // S3 Bucket.
    String bucket;
    // S3 Key (Filename or Path).
    String key;
};

// Regex to parse S3 URL (Endpoint, Bucket, Key).
static const std::regex S3_URL_RE(R"((https?://.*)/(.*)/(.*))");

static S3Endpoint parseFromUrl(const String & url) {
    std::smatch match;
    if (std::regex_search(url, match, S3_URL_RE) && match.size() > 1) {
        S3Endpoint endpoint;
        endpoint.endpoint_url = match.str(1);
        endpoint.bucket = match.str(2);
        endpoint.key = match.str(3);
        return endpoint;
    }
    else
        throw Exception("Failed to parse S3 Storage URL. It should contain endpoint url, bucket and file. "
                        "Regex is (https?://.*)/(.*)/(.*)", ErrorCodes::BAD_ARGUMENTS);
}

/**
 * This class represents table engine for external S3 urls.
 * It sends HTTP GET to server when select is called and
 * HTTP PUT when insert is called.
 */
class StorageS3 : public ext::shared_ptr_helper<StorageS3>, public IStorage
{
public:
    StorageS3(const S3Endpoint & endpoint,
        const std::string & database_name_,
        const std::string & table_name_,
        const String & format_name_,
        UInt64 min_upload_part_size_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        Context & context_,
        const String & compression_method_);

    String getName() const override
    {
        return "S3";
    }

    Block getHeaderBlock(const Names & /*column_names*/) const
    {
        return getSampleBlock();
    }

    String getTableName() const override
    {
        return table_name;
    }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Context & context) override;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name, TableStructureWriteLockHolder &) override;

private:
    S3Endpoint endpoint;
    const Context & context_global;

    String format_name;
    String database_name;
    String table_name;
    UInt64 min_upload_part_size;
    String compression_method;
    std::shared_ptr<Aws::S3::S3Client> client;
};

}
