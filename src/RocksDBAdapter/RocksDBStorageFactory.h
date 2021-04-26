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
 *  @date 20210423
 */
#pragma once

#include "RocksDBStorage.h"
#include "rocksdb/options.h"

namespace rocksdb
{
class DB;
}
namespace bcos
{
namespace storage
{
class RocksDBStorageFactory
{
public:
    RocksDBStorageFactory(const std::string& _dbPath)
      : m_DBPath(_dbPath)
    {}
    virtual ~RocksDBStorageFactory() {}
    RocksDBStorage::Ptr createStorage(const std::string& _dbName, bool _createIfMissing = true);

private:
    const std::string m_DBPath;
};
}  // namespace storage
}  // namespace dev
