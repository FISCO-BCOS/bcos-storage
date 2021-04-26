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
/** @file RocksDBStorageFactory.h
 *  @author xingqiangbai
 *  @date 20180423
 */

#include "RocksDBStorageFactory.h"
#include "RocksDBStorage.h"
#include "boost/filesystem.hpp"
#include "rocksdb/db.h"
#include "rocksdb/slice_transform.h"

using namespace std;
using namespace rocksdb;

namespace bcos
{
namespace storage
{

RocksDBStorage::Ptr RocksDBStorageFactory::createStorage(const std::string& _dbName, bool _createIfMissing)
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
    options.compression = rocksdb::kSnappyCompression;
    // <---- Enable some features supporting prefix extraction
    options.prefix_extractor.reset(NewCappedPrefixTransform(RocksDBStorage::TABLE_PERFIX_LENGTH));

    DB* db;
    Status s = DB::Open(options, dbName, &db);
    std::shared_ptr<RocksDBStorage> rocksdbStorage = std::make_shared<RocksDBStorage>(db);
    return rocksdbStorage;
}
}  // namespace storage
}  // namespace bcos