#include <IO/WriteBufferFromS3.h>

#include <IO/WriteHelpers.h>

#include <Poco/DOM/AutoPtr.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/NodeList.h>
#include <Poco/SAX/InputSource.h>

#include <common/logger_useful.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>


namespace DB
{

const int DEFAULT_S3_MAX_FOLLOW_PUT_REDIRECT = 2;

// S3 protocol does not allow to have multipart upload with more than 10000 parts.
// In case server does not return an error on exceeding that number, we print a warning
// because custom S3 implementation may allow relaxed requirements on that.
const int S3_WARN_MAX_PARTS = 10000;


namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}


WriteBufferFromS3::WriteBufferFromS3(
    std::shared_ptr<Aws::S3::S3Client> & client_ptr_,
    size_t minimum_upload_part_size_,
    size_t buffer_size_
)
    : BufferWithOwnMemory<WriteBuffer>(buffer_size_, nullptr, 0)
    , minimum_upload_part_size {minimum_upload_part_size_}
    , temporary_buffer {std::make_unique<WriteBufferFromString>(buffer_string)}
    , last_part_size {0}
    , client_ptr(client_ptr_)

{
    initiate();
}


void WriteBufferFromS3::nextImpl()
{
    if (!offset())
        return;

    temporary_buffer->write(working_buffer.begin(), offset());

    last_part_size += offset();

    if (last_part_size > minimum_upload_part_size)
    {
        temporary_buffer->finish();
        writePart(buffer_string);
        last_part_size = 0;
        temporary_buffer = std::make_unique<WriteBufferFromString>(buffer_string);
    }
}


void WriteBufferFromS3::finalize()
{
    temporary_buffer->finish();
    if (!buffer_string.empty())
    {
        writePart(buffer_string);
    }

    complete();
}


WriteBufferFromS3::~WriteBufferFromS3()
{
    try
    {
        next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void WriteBufferFromS3::initiate()
{
    // See https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html
    Aws::S3::S3Client client = *client_ptr;
    Aws::S3::Model::CreateMultipartUploadRequest req;

    req.SetBucket("root");
    req.SetKey("test.csv");

    auto outcome = client.CreateMultipartUpload(req);

    if (outcome.IsSuccess()) {
        upload_id = outcome.GetResult().GetUploadId();
    } else {
        throw Exception(outcome.GetError().GetMessage(), 1);
    }
}


void WriteBufferFromS3::writePart(const String & data)
{
    Aws::S3::S3Client client = *client_ptr;
    Aws::S3::Model::UploadPartRequest req;

    req.SetBucket("root");
    req.SetKey("test.csv");
    req.SetPartNumber(part_tags.size() + 1);
    req.SetUploadId(upload_id);
    req.SetContentLength(data.size());

    auto outcome = client.UploadPart(req);

    if (outcome.IsSuccess()) {
        part_tags.push_back(outcome.GetResult().GetETag());
    }
    else {
        throw Exception(outcome.GetError().GetMessage(), 1);
    }
}


void WriteBufferFromS3::complete()
{
    Aws::S3::S3Client client = *client_ptr;
    Aws::S3::Model::CompleteMultipartUploadRequest req;

    Aws::S3::Model::CompletedMultipartUpload upload;
    for (size_t i = 0; i < part_tags.size(); i++) {
        Aws::S3::Model::CompletedPart part;
        upload.AddParts(part.WithETag(part_tags[i]).WithPartNumber(i + 1));
    }

    req.SetBucket("root");
    req.SetKey("test.csv");
    req.SetUploadId(upload_id);
    req.SetMultipartUpload(upload);

    auto outcome = client.CompleteMultipartUpload(req);

    if (!outcome.IsSuccess())
        throw Exception (outcome.GetError().GetMessage(), 1);
}

}
