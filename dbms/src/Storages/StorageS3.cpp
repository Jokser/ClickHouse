#include <Storages/StorageFactory.h>
#include <Storages/StorageS3.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>

#include <IO/ReadBufferFromS3.h>
#include <IO/WriteBufferFromS3.h>

#include <Formats/FormatFactory.h>

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>

#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>

#include <Poco/Net/HTTPRequest.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
    class StorageS3BlockInputStream : public IBlockInputStream
    {
    public:
        StorageS3BlockInputStream(
            const String & format,
            const String & name_,
            const Block & sample_block,
            const Context & context,
            UInt64 max_block_size,
            std::shared_ptr<Aws::S3::S3Client> client)
            : name(name_)
        {
            read_buf = std::make_unique<ReadBufferFromS3>(client);

            reader = FormatFactory::instance().getInput(format, *read_buf, sample_block, context, max_block_size);
        }

        String getName() const override
        {
            return name;
        }

        Block readImpl() override
        {
            return reader->read();
        }

        Block getHeader() const override
        {
            return reader->getHeader();
        }

        void readPrefixImpl() override
        {
            reader->readPrefix();
        }

        void readSuffixImpl() override
        {
            reader->readSuffix();
        }

    private:
        String name;
        std::unique_ptr<ReadBufferFromS3> read_buf;
        BlockInputStreamPtr reader;
    };

    class StorageS3BlockOutputStream : public IBlockOutputStream
    {
    public:
        StorageS3BlockOutputStream(
            const String & format,
            UInt64 min_upload_part_size,
            const Block & sample_block_,
            const Context & context,
            std::shared_ptr<Aws::S3::S3Client> client)
            : sample_block(sample_block_)
        {
            write_buf = std::make_unique<WriteBufferFromS3>(client, min_upload_part_size);
            writer = FormatFactory::instance().getOutput(format, *write_buf, sample_block, context);
        }

        Block getHeader() const override
        {
            return sample_block;
        }

        void write(const Block & block) override
        {
            writer->write(block);
        }

        void writePrefix() override
        {
            writer->writePrefix();
        }

        void writeSuffix() override
        {
            writer->writeSuffix();
            writer->flush();
            write_buf->finalize();
        }

    private:
        Block sample_block;
        std::unique_ptr<WriteBufferFromS3> write_buf;
        BlockOutputStreamPtr writer;
    };
}


StorageS3::StorageS3(
    const std::string & endpoint_url_,
    const std::string & database_name_,
    const std::string & table_name_,
    const String & format_name_,
    UInt64 min_upload_part_size_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    Context & context_)
    : IStorage(columns_)
    , endpoint_url(endpoint_url_)
    , context_global(context_)
    , format_name(format_name_)
    , database_name(database_name_)
    , table_name(table_name_)
    , min_upload_part_size(min_upload_part_size_)
{
    setColumns(columns_);
    setConstraints(constraints_);
    Aws::Client::ClientConfiguration cfg;
    cfg.endpointOverride = endpoint_url;
    cfg.scheme = Aws::Http::Scheme::HTTP;
    client = std::make_shared<Aws::S3::S3Client>(cfg);
}


BlockInputStreams StorageS3::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    BlockInputStreamPtr block_input = std::make_shared<StorageS3BlockInputStream>(
        format_name,
        getName(),
        getHeaderBlock(column_names),
        context,
        max_block_size,
        client);

    auto column_defaults = getColumns().getDefaults();
    if (column_defaults.empty())
        return {block_input};
    return {std::make_shared<AddingDefaultsBlockInputStream>(block_input, column_defaults, context)};
}

void StorageS3::rename(const String & /*new_path_to_db*/, const String & new_database_name, const String & new_table_name, TableStructureWriteLockHolder &)
{
    table_name = new_table_name;
    database_name = new_database_name;
}

BlockOutputStreamPtr StorageS3::write(const ASTPtr & /*query*/, const Context & /*context*/)
{
    return std::make_shared<StorageS3BlockOutputStream>(
        format_name, min_upload_part_size, getSampleBlock(), context_global, client);
}

void registerStorageS3(StorageFactory & factory)
{
    factory.registerStorage("S3", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 2)
            throw Exception(
                "Storage S3 requires exactly 2 arguments: url and name of used format.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.local_context);

        String url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();

        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.local_context);

        String format_name = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

        UInt64 min_upload_part_size = args.local_context.getSettingsRef().s3_min_upload_part_size;

        return StorageS3::create(url, args.database_name, args.table_name, format_name, min_upload_part_size, args.columns, args.constraints, args.context);
    });
}

}
