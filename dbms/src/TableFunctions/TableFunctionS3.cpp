#include <regex>
#include <Storages/StorageS3.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>

namespace DB
{

StoragePtr TableFunctionS3::getStorage(
    const String & source, const String & format, const ColumnsDescription & columns, Context & global_context, const std::string & table_name, const String & compression_method) const
{
    S3Endpoint endpoint = parseFromUrl(source);
    UInt64 min_upload_part_size = global_context.getSettingsRef().s3_min_upload_part_size;
    return StorageS3::create(endpoint, getDatabaseName(), table_name, format, min_upload_part_size, columns, ConstraintsDescription{}, global_context, compression_method);
}

void registerTableFunctionS3(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionS3>();
}

}
