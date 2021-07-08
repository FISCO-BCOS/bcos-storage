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
 *  @date 20210423
 */
#pragma once

#include "RocksDBAdapter.h"
#include "rocksdb/db.h"
#include <utility>

namespace rocksdb
{
class DB;
}
namespace bcos
{
namespace storage
{
class RocksDBAdapterFactory
{
public:
    explicit RocksDBAdapterFactory(const std::string& _dbPath) : m_DBPath(_dbPath) {}
    virtual ~RocksDBAdapterFactory() {}
    RocksDBAdapter::Ptr createAdapter(const std::string& _dbName, int _perfixLength = 0);
    std::pair<rocksdb::DB*, std::vector<rocksdb::ColumnFamilyHandle*>> createRocksDB(
        const std::string& _dbName, int _perfixLength = 0, bool _createIfMissing = true,
        const std::vector<std::string>& _columnFamilies = std::vector<std::string>{});

private:
    const std::string m_DBPath;
};
}  // namespace storage
}  // namespace bcos
