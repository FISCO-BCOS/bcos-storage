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
/** @file RocksDBAdapter.cpp
 *  @author xingqiangbai
 *  @date 20180423
 */

#include "RocksDBAdapter.h"
#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/iostreams/device/back_inserter.hpp"
#include "boost/iostreams/stream_buffer.hpp"
#include "boost/lexical_cast.hpp"
#include "boost/serialization/vector.hpp"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "tbb/parallel_for.h"
#include "tbb/spin_mutex.h"
#include <memory>
#include <thread>

using namespace std;
using namespace rocksdb;

namespace bcos
{
namespace storage
{
using string_buf =
    boost::iostreams::stream_buffer<boost::iostreams::back_insert_device<std::string>>;
const char* const METADATA_COLUMN_NAME = "meta";
const char* const CURRENT_TABLE_ID = "tableID";
const string TABLE_PERFIX = "t";
const char* const TABLE_KEY_PERFIX = "k";

RocksDBAdapter::RocksDBAdapter(rocksdb::DB* _db) : m_db(_db)
{
    auto s = m_db->CreateColumnFamily(ColumnFamilyOptions(), METADATA_COLUMN_NAME, &m_metadataCF);
    assert(s.ok());
}

RocksDBAdapter::~RocksDBAdapter()
{
    if (m_metadataCF)
    {
        auto s = m_db->DestroyColumnFamilyHandle(m_metadataCF);
        assert(s.ok());
    }
}

std::pair<std::string, bool> RocksDBAdapter::getTablePerfix(const std::string& _tableName) const
{
    // perfix+tableName store tableID, store tableID in meta column Family
    // tableID use 8B
    static string perfix("t");
    auto realKey = perfix + _tableName;

    {  // query from cache
        std::shared_lock lock(m_tableIDCacheMutex);
        if (m_tableIDCache.count(realKey))
        {
            return std::make_pair(m_tableIDCache[realKey], true);
        }
    }

    // if cache is missed, query from rocksDB
    string value;
    auto status = m_db->Get(ReadOptions(), m_metadataCF, Slice(realKey), &value);
    if (!status.ok())
    {  // panic
        STORAGE_LOG(ERROR) << LOG_BADGE("RocksDBAdapter getTablePerfix failed") << LOG_DESC("table not exist")
                           << LOG_KV("name", _tableName) << LOG_KV("message", status.ToString());
        return std::make_pair("", false);
    }
    value = perfix + value;
    {  // insert into cache
        std::unique_lock lock(m_tableIDCacheMutex);
        m_tableIDCache[realKey] = value;
    }
    return std::make_pair(value, true);
}

int64_t RocksDBAdapter::getNextTableID()
{
    if (m_tableID.load() == 0)
    {  // get table Id from database
        string value;
        auto status = m_db->Get(ReadOptions(), m_metadataCF, Slice(CURRENT_TABLE_ID), &value);
        if (status.IsNotFound())
        {  // the first time to get the table id
            m_tableID.store(2);
            return 1;
        }
        // convert current table id string to int64_t
        int64_t nextTableID = boost::lexical_cast<int64_t>(value) + 1;
        // store table id in m_tableID
        m_tableID.store(nextTableID);
        return nextTableID;
    }
    return m_tableID.fetch_add(1);
}

std::vector<std::string> RocksDBAdapter::getPrimaryKeys(
    std::shared_ptr<TableInfo> _tableInfo, std::shared_ptr<Condition> _condition) const
{
    vector<string> ret;
    // get TableID according tableName,
    auto perfixPair = getTablePerfix(_tableInfo->name);
    if (!perfixPair.second)
    {
        STORAGE_LOG(DEBUG) << LOG_BADGE("RocksDBAdapter") << LOG_DESC("getPrimaryKeys failed")
                           << LOG_KV("name", _tableInfo->name);
        return ret;
    }
    auto& tablePerfix = perfixPair.first;
    // perfix query
    ReadOptions read_options;
    read_options.auto_prefix_mode = true;
    auto iter = m_db->NewIterator(read_options);
    for (iter->Seek(tablePerfix); iter->Valid() && iter->key().starts_with(tablePerfix);
         iter->Next())
    {
        if (_condition->isValid(string_view(iter->key().data(), iter->key().size())))
        {  // filter by condition
            ret.emplace_back(iter->key().ToString());
        }
    }
    return ret;
}

std::shared_ptr<Entry> RocksDBAdapter::getRow(
    std::shared_ptr<TableInfo>& _tableInfo, const std::string_view& _key)
{
    // get TableID according tableName,
    auto perfixPair = getTablePerfix(_tableInfo->name);
    if (!perfixPair.second)
    {
        return nullptr;
    }
    // construct the real key and get
    auto& realKey = perfixPair.first.append(_key);
    string value;
    auto status = m_db->Get(ReadOptions(), Slice(realKey), &value);
    if (!status.ok())
    {
        STORAGE_LOG(DEBUG) << LOG_BADGE("RocksDBAdapter") << LOG_DESC("getRow failed")
                           << LOG_KV("name", _tableInfo->name) << LOG_KV("key", _key)
                           << LOG_KV("message", status.ToString());
        return nullptr;
    }
    // deserialization the value to vector
    vector<string> res;
    stringstream ss(value);
    boost::archive::binary_iarchive ia(ss);
    ia >> res;
    // according to the table info construct an entry
    return vectorToEntry(_tableInfo, res);
}

std::map<std::string, std::shared_ptr<Entry>> RocksDBAdapter::getRows(
    std::shared_ptr<TableInfo>& _tableInfo, const std::vector<std::string>& _keys)
{
    std::map<std::string, std::shared_ptr<Entry>> ret;
    if (_keys.empty())
    {
        return ret;
    }
    // get TableID according tableName,
    auto perfixPair = getTablePerfix(_tableInfo->name);
    if (!perfixPair.second)
    {
        return ret;
    }
    auto tablePerfix = std::move(perfixPair.first);
    // construct the real key and batch get
    vector<Slice> keys;
    keys.reserve(_keys.size());
    for (auto& key : _keys)
    {
        keys.emplace_back(tablePerfix + TABLE_KEY_PERFIX + key);
    }
    vector<string> values;
    auto status = m_db->MultiGet(ReadOptions(), keys, &values);
    for (size_t i = 0; i < status.size(); ++i)
    {
        if (status[i].ok())
        {
            vector<string> res;
            stringstream ss(values[i]);
            boost::archive::binary_iarchive ia(ss);
            ia >> res;
            ret.insert(std::make_pair(keys[i].ToString(), vectorToEntry(_tableInfo, res)));
        }
        else
        {
            ret.insert(std::make_pair(keys[i].ToString(), nullptr));
        }
    }
    return ret;
}

size_t RocksDBAdapter::commitTables(const std::vector<std::shared_ptr<TableInfo>> _tableInfos,
    std::vector<std::shared_ptr<std::map<std::string, std::shared_ptr<Entry>>>>& _tableDatas)
{
    atomic<size_t> total = 0;
    if (_tableInfos.size() != _tableDatas.size())
    {
        // TODO:  panic
        STORAGE_LOG(ERROR) << LOG_BADGE("RocksDBAdapter") << LOG_DESC("commitTables info and data size mismatch");
        return 0;
    }
    WriteBatch writeBatch;
    tbb::spin_mutex batchMutex;
    // assign tableID for new tables
    tbb::parallel_for(tbb::blocked_range<size_t>(0, _tableInfos.size()),
        [&](const tbb::blocked_range<size_t>& range) {
            for (size_t i = range.begin(); i < range.end(); ++i)
            {
                auto tableInfo = _tableInfos[i];
                auto tablePerfix = TABLE_PERFIX;
                if (tableInfo->newTable)
                {
                    auto tableID = getNextTableID();
                    auto realKey = TABLE_PERFIX + tableInfo->name;
                    tablePerfix.append((char*)&tableID, sizeof(tableID));
                    {  // insert into cache
                        std::unique_lock lock(m_tableIDCacheMutex);
                        m_tableIDCache[realKey] = tablePerfix;
                    }
                    {  // put new tableID to write batch
                        tbb::spin_mutex::scoped_lock lock(batchMutex);
                        writeBatch.Put(m_metadataCF, Slice(realKey), Slice(tablePerfix));
                    }
                }
                else
                {
                    auto perfixPair = getTablePerfix(tableInfo->name);
                    if (!perfixPair.second)
                    {  // TODO: panic
                        STORAGE_LOG(ERROR) << LOG_BADGE("RocksDBAdapter") << LOG_DESC("commitTables info and data size mismatch");
                    }
                    tablePerfix = std::move(perfixPair.first);
                }
                // parallel process tables data, convert to map of key value string
                auto tableData = _tableDatas[i];
                for (auto& data : *tableData)
                {
                    auto realKey = tablePerfix + data.first;
                    vector<string> values;
                    values.reserve(data.second->size());
                    values.emplace_back(data.second->getField(tableInfo->key));
                    for (auto& columnName : tableInfo->fields)
                    {
                        values.emplace_back(data.second->getField(columnName));
                    }
                    string serializedValue;
                    string_buf buf(serializedValue);
                    std::ostream os(&buf);
                    boost::archive::binary_oarchive oa(os);
                    oa << values;
                    {
                        tbb::spin_mutex::scoped_lock lock(batchMutex);
                        writeBatch.Put(Slice(realKey), Slice(serializedValue));
                    }
                    total.fetch_add(1);
                }
                // TODO: maybe commit table to different column family according second path
            }
        });
    // commit current tableID in meta column family
    auto currentTableID = m_tableID.load() - 1;
    writeBatch.Put(m_metadataCF, Slice(CURRENT_TABLE_ID), Slice(to_string(currentTableID)));
    // commit to rocksDB
    auto status = m_db->Write(WriteOptions(), &writeBatch);
    if (!status.ok())
    {
        // panic
        return 0;
    }
    return total.load();
}

}  // namespace storage
}  // namespace bcos
