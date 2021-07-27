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
/** @file RocksDBAdapter.h
 *  @author xingqiangbai
 *  @date 20180423
 */
#pragma once

#include "bcos-storage/AdapterInterface.h"
#include <boost/algorithm/string/join.hpp>
#include <boost/lexical_cast.hpp>
#include <atomic>
#include <map>
#include <shared_mutex>

namespace rocksdb
{
class DB;
class ColumnFamilyHandle;
}  // namespace rocksdb

namespace bcos
{
namespace storage
{
const char* const METADATA_COLUMN_NAME = "meta";
class RocksDBAdapter : public AdapterInterface
{
public:
    static const int TABLE_PREFIX_LENGTH = 9;  //"t" + sizeof(int64_t) + "k"
    // using CryptHandler = std::function<void(std::string const&, std::string&)>;
    typedef std::shared_ptr<RocksDBAdapter> Ptr;
    explicit RocksDBAdapter(rocksdb::DB* _db, rocksdb::ColumnFamilyHandle* handler);
    virtual ~RocksDBAdapter();

    std::vector<std::string> getPrimaryKeys(
        const TableInfo::Ptr& _tableInfo, const Condition::Ptr& _condition) const override;
    Entry::Ptr getRow(const TableInfo::Ptr& _tableInfo, const std::string_view& _key) override;
    std::map<std::string, Entry::Ptr> getRows(
        const TableInfo::Ptr& _tableInfo, const std::vector<std::string>& _keys) override;
    std::pair<size_t, Error::Ptr> commitTables(
        const std::vector<std::shared_ptr<TableInfo>>& _tableInfos,
        const std::vector<std::shared_ptr<std::map<std::string, Entry::Ptr>>>& _tableDatas)
        override;

private:
    inline std::pair<std::string, bool> getTablePrefix(const std::string& _tableName) const;
    inline int64_t getNextTableID();

    inline Entry::Ptr vectorToEntry(
        const TableInfo::Ptr& _tableInfo, std::vector<std::string>& _values) const
    {
        if (_tableInfo->fields.size() != _values.size() - 3)
        {  // panic, 3 means [key, status, num]
            STORAGE_LOG(ERROR) << LOG_BADGE("RocksDBAdapter data mismatch")
                               << LOG_KV("name", _tableInfo->name)
                               << LOG_KV("expect", _tableInfo->fields.size() + 3)
                               << LOG_KV("got", _values.size())
                               << LOG_KV("values", boost::algorithm::join(_values, "|"))
                               << LOG_KV("fields", boost::algorithm::join(_tableInfo->fields, "|"));
            return nullptr;
        }
        auto deleted = boost::lexical_cast<bool>(_values[_tableInfo->fields.size() + 1]);
        if (deleted)
        {  // deleted entry should not exist in rocksDB
            STORAGE_LOG(ERROR) << LOG_BADGE("RocksDBAdapter found deleted entry in DB")
                               << LOG_KV("name", _tableInfo->name) << LOG_KV("key", _values[0]);
            return nullptr;
        }
        auto entry = std::make_shared<Entry>();
        entry->setField(_tableInfo->key, std::move(_values[0]));
        for (size_t i = 0; i < _tableInfo->fields.size(); ++i)
        {
            entry->setField(_tableInfo->fields[i], std::move(_values[i + 1]));
        }
        auto number =
            boost::lexical_cast<protocol::BlockNumber>(_values[_tableInfo->fields.size() + 2]);
        entry->setNum(number);
        entry->setDirty(false);
        return entry;
    }

    std::unique_ptr<rocksdb::DB> m_db;
    rocksdb::ColumnFamilyHandle* m_metadataCF = nullptr;
    mutable std::shared_mutex m_tableIDCacheMutex;
    mutable std::map<std::string, std::string> m_tableIDCache;
    std::atomic<int64_t> m_tableID = {0};
};

}  // namespace storage

}  // namespace bcos
