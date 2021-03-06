#include <Databases/DatabasesCommon.h>

#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ExternalLoaderDatabaseConfigRepository.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDictionary.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageFactory.h>
#include <Common/typeid_cast.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Dictionaries/DictionaryFactory.h>

#include <sstream>


namespace DB
{

namespace ErrorCodes
{
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int DICTIONARY_ALREADY_EXISTS;
}

namespace
{

StoragePtr getDictionaryStorage(const Context & context, const String & table_name, const String & db_name)
{
    auto dict_name = db_name + "." + table_name;
    const auto & external_loader = context.getExternalDictionariesLoader();
    auto dict_ptr = external_loader.tryGetDictionary(dict_name);
    if (dict_ptr)
    {
        const DictionaryStructure & dictionary_structure = dict_ptr->getStructure();
        auto columns = StorageDictionary::getNamesAndTypes(dictionary_structure);
        return StorageDictionary::create(db_name, table_name, ColumnsDescription{columns}, context, true, dict_name);
    }
    return nullptr;
}

}

bool DatabaseWithOwnTablesBase::isTableExist(
    const Context & /*context*/,
    const String & table_name) const
{
    std::lock_guard lock(mutex);
    return tables.find(table_name) != tables.end() || dictionaries.find(table_name) != dictionaries.end();
}

bool DatabaseWithOwnTablesBase::isDictionaryExist(
    const Context & /*context*/,
    const String & dictionary_name) const
{
    std::lock_guard lock(mutex);
    return dictionaries.find(dictionary_name) != dictionaries.end();
}

StoragePtr DatabaseWithOwnTablesBase::tryGetTable(
    const Context & context,
    const String & table_name) const
{
    {
        std::lock_guard lock(mutex);
        auto it = tables.find(table_name);
        if (it != tables.end())
            return it->second;
    }

    if (isDictionaryExist(context, table_name))
        /// We don't need lock database here, because database doesn't store dictionary itself
        /// just metadata
        return getDictionaryStorage(context, table_name, getDatabaseName());

    return {};
}

DatabaseTablesIteratorPtr DatabaseWithOwnTablesBase::getTablesWithDictionaryTablesIterator(const Context & context, const FilterByNameFunction & filter_by_name)
{
    auto tables_it = getTablesIterator(context, filter_by_name);
    auto dictionaries_it = getDictionariesIterator(context, filter_by_name);

    Tables result;
    while (tables_it && tables_it->isValid())
    {
        result.emplace(tables_it->name(), tables_it->table());
        tables_it->next();
    }

    while (dictionaries_it && dictionaries_it->isValid())
    {
        auto table_name = dictionaries_it->name();
        auto table_ptr = getDictionaryStorage(context, table_name, getDatabaseName());
        if (table_ptr)
            result.emplace(table_name, table_ptr);
        dictionaries_it->next();
    }

    return std::make_unique<DatabaseTablesSnapshotIterator>(result);
}

DatabaseTablesIteratorPtr DatabaseWithOwnTablesBase::getTablesIterator(const Context & /*context*/, const FilterByNameFunction & filter_by_table_name)
{
    std::lock_guard lock(mutex);
    if (!filter_by_table_name)
        return std::make_unique<DatabaseTablesSnapshotIterator>(tables);

    Tables filtered_tables;
    for (const auto & [table_name, storage] : tables)
        if (filter_by_table_name(table_name))
            filtered_tables.emplace(table_name, storage);

    return std::make_unique<DatabaseTablesSnapshotIterator>(std::move(filtered_tables));
}


DatabaseDictionariesIteratorPtr DatabaseWithOwnTablesBase::getDictionariesIterator(const Context & /*context*/, const FilterByNameFunction & filter_by_dictionary_name)
{
    std::lock_guard lock(mutex);
    if (!filter_by_dictionary_name)
        return std::make_unique<DatabaseDictionariesSnapshotIterator>(dictionaries);

    Dictionaries filtered_dictionaries;
    for (const auto & dictionary_name : dictionaries)
        if (filter_by_dictionary_name(dictionary_name))
            filtered_dictionaries.emplace(dictionary_name);
    return std::make_unique<DatabaseDictionariesSnapshotIterator>(std::move(filtered_dictionaries));
}

bool DatabaseWithOwnTablesBase::empty(const Context & /*context*/) const
{
    std::lock_guard lock(mutex);
    return tables.empty() && dictionaries.empty();
}

StoragePtr DatabaseWithOwnTablesBase::detachTable(const String & table_name)
{
    StoragePtr res;
    {
        std::lock_guard lock(mutex);
        if (dictionaries.count(table_name))
            throw Exception("Cannot detach dictionary " + name + "." + table_name + " as table, use DETACH DICTIONARY query.", ErrorCodes::UNKNOWN_TABLE);

        auto it = tables.find(table_name);
        if (it == tables.end())
            throw Exception("Table " + name + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
        res = it->second;
        tables.erase(it);
    }

    return res;
}

void DatabaseWithOwnTablesBase::detachDictionary(const String & dictionary_name, const Context & context, bool reload)
{
    {
        std::lock_guard lock(mutex);
        auto it = dictionaries.find(dictionary_name);
        if (it == dictionaries.end())
            throw Exception("Dictionary " + name + "." + dictionary_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
        dictionaries.erase(it);
    }

    if (reload)
        context.getExternalDictionariesLoader().reload(getDatabaseName() + "." + dictionary_name);

}

void DatabaseWithOwnTablesBase::attachTable(const String & table_name, const StoragePtr & table)
{
    std::lock_guard lock(mutex);
    if (!tables.emplace(table_name, table).second)
        throw Exception("Table " + name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
}


void DatabaseWithOwnTablesBase::attachDictionary(const String & dictionary_name, const Context & context, bool load)
{
    const auto & external_loader = context.getExternalDictionariesLoader();

    String full_name = getDatabaseName() + "." + dictionary_name;
    {
        std::lock_guard lock(mutex);
        auto status = external_loader.getCurrentStatus(full_name);
        if (status != ExternalLoader::Status::NOT_EXIST || !dictionaries.emplace(dictionary_name).second)
            throw Exception(
                      "Dictionary " + full_name + " already exists.",
                      ErrorCodes::DICTIONARY_ALREADY_EXISTS);
    }

    if (load)
        external_loader.reload(full_name, true);
}

void DatabaseWithOwnTablesBase::shutdown()
{
    /// You can not hold a lock during shutdown.
    /// Because inside `shutdown` function tables can work with database, and mutex is not recursive.

    Tables tables_snapshot;
    {
        std::lock_guard lock(mutex);
        tables_snapshot = tables;
    }

    for (const auto & kv : tables_snapshot)
    {
        kv.second->shutdown();
    }

    std::lock_guard lock(mutex);
    tables.clear();
    dictionaries.clear();
}

DatabaseWithOwnTablesBase::~DatabaseWithOwnTablesBase()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
