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
#include "rocksdb/slice_transform.h"

using namespace std;
using namespace rocksdb;

namespace bcos
{
namespace storage
{
rocksdb::DB* RocksDBAdapterFactory::createRocksDB(
    const std::string& _dbName, int _perfixLength, bool _createIfMissing)
{
    auto dbName = m_DBPath + "/" + _dbName;
    if (!_createIfMissing && !boost::filesystem::exists(dbName))
    {
        return nullptr;
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

    DB* db;
    Status s = DB::Open(options, dbName, &db);
    if (!s.ok())
    {
        STORAGE_LOG(ERROR) << LOG_BADGE("RocksDBAdapterFactory open rocksDB failed")
                           << LOG_KV("dbName", dbName) << LOG_KV("message", s.ToString());
        return nullptr;
    }
    return db;
}

RocksDBAdapter::Ptr RocksDBAdapterFactory::createAdapter(
    const std::string& _dbName, int _perfixLength)
{
    auto db = createRocksDB(_dbName, _perfixLength);
    std::shared_ptr<RocksDBAdapter> rocksdbStorage = std::make_shared<RocksDBAdapter>(db);
    return rocksdbStorage;
}
}  // namespace storage
}  // namespace bcos