#pragma once

#include <memory>

#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBuffer.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/URI.h>
#include <aws/s3/S3Client.h>

namespace DB
{
/** Perform S3 HTTP GET request and provide response to read.
  */
class ReadBufferFromS3 : public ReadBuffer
{
protected:
    std::istream * istr; /// owned by session
    std::unique_ptr<ReadBuffer> impl;

public:
    explicit ReadBufferFromS3(const std::shared_ptr<Aws::S3::S3Client> & clientPtr,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    bool nextImpl() override;
};

}
