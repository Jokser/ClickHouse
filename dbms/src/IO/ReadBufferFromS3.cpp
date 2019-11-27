#include <IO/ReadBufferFromS3.h>

#include <IO/ReadBufferFromIStream.h>

#include <common/logger_useful.h>
#include <aws/s3/model/GetObjectRequest.h>


namespace DB
{

const int DEFAULT_S3_MAX_FOLLOW_GET_REDIRECT = 2;

ReadBufferFromS3::ReadBufferFromS3(const std::shared_ptr<Aws::S3::S3Client> & clientPtr, size_t buffer_size_)
    : ReadBuffer(nullptr, 0)
{
    Aws::S3::S3Client client = *clientPtr;

    Aws::S3::Model::GetObjectRequest req;

    req.SetBucket("root");
    req.SetKey("test.csv");
    Aws::S3::Model::GetObjectOutcome outcome = client.GetObject(req);
    if (outcome.IsSuccess()) {
        istr = &outcome.GetResult().GetBody();
        impl = std::make_unique<ReadBufferFromIStream>(*istr, buffer_size_);
    }
    else {
        throw Exception(outcome.GetError().GetMessage(), 1);
    }
}

bool ReadBufferFromS3::nextImpl()
{
    if (!impl->next())
        return false;
    internal_buffer = impl->buffer();
    working_buffer = internal_buffer;
    return true;
}

}
