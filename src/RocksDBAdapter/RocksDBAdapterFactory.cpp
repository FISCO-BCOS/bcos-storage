/*
 * @CopyRight:
 * FISCO-BCOS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * FISCO-BCOS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with FISCO-BCOS.  If not, see <http://www.gnu.org/licenses/>
 * (c) 2016-2018 fisco-dev contributors.
 */
/** @file RocksDBAdapterFactory.h
 *  @author xingqiangbai
 *  @date 20180423
 */

#include "RocksDBAdapterFactory.h"
#include "RocksDBAdapter.h"
#include "boost/filesystem.hpp"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"

using namespace std;
using namespace rocksdb;

namespace bcos
{
namespace storage
{
std::pair<rocksdb::DB*, std::vector<rocksdb::ColumnFamilyHandle*>>
RocksDBAdapterFactory::createRocksDB(const std::string& _dbName, int _perfixLength,
    bool _createIfMissing, const std::vector<std::string>& _columnFamilies)
{
    std::pair<rocksdb::DB*, std::vector<rocksdb::ColumnFamilyHandle*>> ret{
        nullptr, vector<ColumnFamilyHandle*>()};

    auto dbName = m_DBPath + "/" + _dbName;
    if (!_createIfMissing && !boost::filesystem::exists(dbName))
    {
        STORAGE_LOG(ERROR) << LOG_BADGE("RocksDBAdapterFactory open rocksDB failed, path not exist")
                           << LOG_KV("dbName", dbName);
        return ret;
    }

    boost::filesystem::create_directories(dbName);
    Options options;
    options.create_if_missing = true;
    options.max_open_files = 200;
    options.compression = rocksdb::kZSTD;
    if (_perfixLength > 0)
    {  // supporting prefix extraction
        options.prefix_extractor.reset(NewCappedPrefixTransform(_perfixLength));
    }
    if (_columnFamilies.empty())
    {
        Status s = DB::Open(options, dbName, &ret.first);
        if (!s.ok() && !ret.first)
        {
            STORAGE_LOG(ERROR) << LOG_BADGE("open rocksDB failed") << LOG_KV("dbName", dbName)
                               << LOG_KV("message", s.ToString());
        }
        return ret;
    }

    std::vector<ColumnFamilyDescriptor> column_families;

    std::vector<std::string> currentColumnFamilies;
    DB::ListColumnFamilies(options, dbName, &currentColumnFamilies);
    // the first open get ListColumnFamilies don't has default
    column_families.push_back(
        ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, ColumnFamilyOptions()));
    for (auto& columnName : currentColumnFamilies)
    {
        // STORAGE_LOG(ERROR) << columnName << endl;
        if (columnName == rocksdb::kDefaultColumnFamilyName)
        {
            continue;
        }
        column_families.push_back(ColumnFamilyDescriptor(columnName, ColumnFamilyOptions()));
    }

    DB* db = nullptr;
    std::vector<rocksdb::ColumnFamilyHandle*> tempHandlers;
    Status s = DB::Open(options, dbName, column_families, &tempHandlers, &db);
    if (!s.ok() && !db)
    {
        STORAGE_LOG(ERROR) << LOG_BADGE("open rocksDB failed") << LOG_KV("dbName", dbName)
                           << LOG_KV("message", s.ToString());
    }
    assert(db);
    for (auto& name : _columnFamilies)
    {
        if (find(currentColumnFamilies.begin(), currentColumnFamilies.end(), name) ==
            currentColumnFamilies.end())
        {
            if (name == rocksdb::kDefaultColumnFamilyName)
            {
                continue;
            }
            ColumnFamilyHandle* cf;
            db->CreateColumnFamily(ColumnFamilyOptions(), name, &cf);
            db->DestroyColumnFamilyHandle(cf);
            column_families.push_back(ColumnFamilyDescriptor(name, ColumnFamilyOptions()));
        }
    }
    for (auto& handler : tempHandlers)
    {
        auto s = db->DestroyColumnFamilyHandle(handler);
        assert(s.ok());
    }
    delete db;
    s = DB::Open(options, dbName, column_families, &ret.second, &ret.first);
    if (!s.ok())
    {
        STORAGE_LOG(ERROR) << LOG_BADGE("open rocksDB with columnFamilies failed")
                           << LOG_KV("dbName", dbName) << LOG_KV("message", s.ToString());
    }
    return ret;
}

RocksDBAdapter::Ptr RocksDBAdapterFactory::createAdapter(
    const std::string& _dbName, int _perfixLength)
{
    vector<string> columnFamilies{METADATA_COLUMN_NAME};
    auto ret = createRocksDB(_dbName, _perfixLength, true, columnFamilies);
    assert(ret.first);
    assert(ret.second.size() == 2);
    // 0 is the default column family
    ret.first->DestroyColumnFamilyHandle(ret.second[0]);
    std::shared_ptr<RocksDBAdapter> rocksdbStorage =
        std::make_shared<RocksDBAdapter>(ret.first, ret.second.back());
    return rocksdbStorage;
}
}  // namespace storage
}  // namespace bcos