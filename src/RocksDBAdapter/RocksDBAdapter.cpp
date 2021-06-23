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
#include "bcos-framework/interfaces/storage/TableInterface.h"
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
#include "tbb/parallel_for_each.h"
#include "tbb/spin_mutex.h"
#include <memory>
#include <mutex>
#include <thread>

using namespace std;
using namespace rocksdb;

namespace bcos
{
namespace storage
{
using string_buf =
    boost::iostreams::stream_buffer<boost::iostreams::back_insert_device<std::string>>;

const char* const CURRENT_TABLE_ID = "tableID";
const char* const TABLE_PERFIX = "t";
const char* const TABLE_KEY_PERFIX = "k";

RocksDBAdapter::RocksDBAdapter(rocksdb::DB* _db, rocksdb::ColumnFamilyHandle* _handler)
  : m_db(_db), m_metadataCF(_handler)
{
    int64_t tableID = 0;
    auto realKey = string(TABLE_PERFIX) + SYS_TABLE;
    string tablePerfix = TABLE_PERFIX;
    tablePerfix.append((char*)&tableID, sizeof(tableID));

    {  // insert into cache
        m_tableIDCache[realKey] = tablePerfix;
    }
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
    // TABLE_PERFIX+tableName store tableID, store tableID in meta column Family
    // tableID use 8B

    auto realKey = TABLE_PERFIX + _tableName;

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
        STORAGE_LOG(ERROR) << LOG_BADGE("RocksDBAdapter") << LOG_DESC("table not exist")
                           << LOG_KV("name", _tableName) << LOG_KV("message", status.ToString());
        return std::make_pair("", false);
    }
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
    const TableInfo::Ptr& _tableInfo, const Condition::Ptr& _condition) const
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
    // TABLE_PERFIX query
    ReadOptions read_options;
    read_options.auto_prefix_mode = true;
    auto iter = m_db->NewIterator(read_options);
    for (iter->Seek(tablePerfix); iter->Valid() && iter->key().starts_with(tablePerfix);
         iter->Next())
    {
        size_t start = TABLE_PERFIX_LENGTH + 1;  // 1 is length of TABLE_KEY_PERFIX
        if (!_condition || _condition->isValid(
                               string_view(iter->key().data() + start, iter->key().size() - start)))
        {  // filter by condition, the key need remove TABLE_PERFIX
            ret.emplace_back(iter->key().ToString().substr(start));
        }
    }
    delete iter;
    return ret;
}

Entry::Ptr RocksDBAdapter::getRow(const TableInfo::Ptr& _tableInfo, const std::string_view& _key)
{
    // get TableID according tableName,
    auto perfixPair = getTablePerfix(_tableInfo->name);
    if (!perfixPair.second)
    {
        return nullptr;
    }
    // construct the real key and get
    auto& realKey = perfixPair.first.append(TABLE_KEY_PERFIX).append(_key);
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

std::map<std::string, Entry::Ptr> RocksDBAdapter::getRows(
    const TableInfo::Ptr& _tableInfo, const std::vector<std::string>& _keys)
{
    std::map<std::string, Entry::Ptr> ret;
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
    vector<string> realkeys(_keys.size(), tablePerfix + TABLE_KEY_PERFIX);
    tbb::parallel_for(
        tbb::blocked_range<size_t>(0, _keys.size()), [&](const tbb::blocked_range<size_t>& range) {
            for (auto it = range.begin(); it != range.end(); ++it)
            {
                realkeys[it].append(_keys[it]);
            }
        });

    vector<Slice> keys(_keys.size(), Slice());
    tbb::parallel_for(
        tbb::blocked_range<size_t>(0, _keys.size()), [&](const tbb::blocked_range<size_t>& range) {
            for (auto it = range.begin(); it != range.end(); ++it)
            {
                keys[it] = Slice(realkeys[it]);
            }
        });
    vector<string> values;
    auto status = m_db->MultiGet(ReadOptions(), keys, &values);
    mutex retMutex;
    tbb::parallel_for(
        tbb::blocked_range<size_t>(0, _keys.size()), [&](const tbb::blocked_range<size_t>& range) {
            for (auto it = range.begin(); it != range.end(); ++it)
            {
                if (status[it].ok())
                {
                    vector<string> res;
                    stringstream ss(values[it]);
                    boost::archive::binary_iarchive ia(ss);
                    ia >> res;
                    {
                        std::lock_guard<std::mutex> lock(retMutex);
                        ret.insert(std::make_pair(_keys[it], vectorToEntry(_tableInfo, res)));
                    }
                }
                else
                {
                    STORAGE_LOG(ERROR)
                        << LOG_BADGE("RocksDBAdapter getRows error")
                        << LOG_KV("name", _tableInfo->name) << LOG_KV("key", _keys[it])
                        << LOG_KV("message", status[it].ToString());
                    {
                        std::lock_guard<std::mutex> lock(retMutex);
                        ret.insert(std::make_pair(_keys[it], nullptr));
                    }
                }
            }
        });
    return ret;
}

std::pair<size_t, Error::Ptr> RocksDBAdapter::commitTables(
    const std::vector<std::shared_ptr<TableInfo>>& _tableInfos,
    const std::vector<std::shared_ptr<std::map<std::string, Entry::Ptr>>>& _tableDatas)
{
    atomic<size_t> total = 0;
    if (_tableInfos.size() != _tableDatas.size())
    {  // panic
        STORAGE_LOG(ERROR) << LOG_BADGE("RocksDBAdapter")
                           << LOG_DESC("commitTables info and data size mismatch");
        return {
            0, make_shared<Error>(StorageErrorCode::InvalidArgument, "parameters size mismatch")};
    }
    assert(_tableInfos.size() == _tableDatas.size());

    auto start_time = utcTime();
    WriteBatch writeBatch;
    tbb::spin_mutex batchMutex;

    // assign tableID for new tables
    tbb::parallel_for(tbb::blocked_range<size_t>(0, _tableInfos.size()),
        [&](const tbb::blocked_range<size_t>& range) {
            for (size_t i = range.begin(); i < range.end(); ++i)
            {
                auto tableInfo = _tableInfos[i];
                if (tableInfo->name == SYS_TABLE)
                {
                    auto data = _tableDatas[i];
                    tbb::parallel_for_each(data->begin(), data->end(),
                        [&](std::pair<const std::string, Entry::Ptr>& item) {
                            // the entry in SYS_TABLE is always new entry
                            auto tableID = getNextTableID();
                            auto realKey = TABLE_PERFIX + item.first;
                            string tablePerfix = TABLE_PERFIX;
                            tablePerfix.append((char*)&tableID, sizeof(tableID));

                            {  // insert into cache
                                std::unique_lock lock(m_tableIDCacheMutex);
                                m_tableIDCache[realKey] = tablePerfix;
                            }
                            {  // put new tableID to write batch
                                // storage doesn't promiss SYS_TABLE is unique
                                tbb::spin_mutex::scoped_lock lock(batchMutex);
                                writeBatch.Put(m_metadataCF, Slice(realKey), Slice(tablePerfix));
                            }
                            STORAGE_LOG(TRACE)
                                << LOG_BADGE("RocksDBAdapter new table")
                                << LOG_KV("name", tableInfo->name) << LOG_KV("id", tableID)
                                << LOG_KV("key", item.first);
                        });
                }
            }
        });
    auto assignID_time_cost = utcTime();
    // process data cost a lot of time, parallel the serialization
    tbb::parallel_for(tbb::blocked_range<size_t>(0, _tableInfos.size()),
        [&](const tbb::blocked_range<size_t>& range) {
            for (size_t i = range.begin(); i < range.end(); ++i)
            {
                auto tableInfo = _tableInfos[i];
                string tablePerfix = TABLE_PERFIX;
                auto perfixPair = getTablePerfix(tableInfo->name);
                if (!perfixPair.second)
                {  // panic
                    STORAGE_LOG(ERROR)
                        << LOG_BADGE("commitTables") << LOG_DESC("getTablePerfix failed")
                        << LOG_KV("name", tableInfo->name);
                }
                assert(perfixPair.second);
                tablePerfix = std::move(perfixPair.first);
                // parallel process tables data, convert to map of key value string
                auto tableData = _tableDatas[i];
                tbb::parallel_for_each(tableData->begin(), tableData->end(),
                    [&](std::pair<const std::string, Entry::Ptr>& data) {
                        auto realKey = tablePerfix + TABLE_KEY_PERFIX + data.first;
                        if (data.second->getStatus() == Entry::Status::DELETED)
                        {  // deleted entry should use batch Delete
                            tbb::spin_mutex::scoped_lock lock(batchMutex);
                            writeBatch.Delete(Slice(realKey));
                        }
                        else
                        {
                            vector<string> values;
                            values.reserve(data.second->size());
                            values.emplace_back(data.second->getField(tableInfo->key));

                            for (auto& columnName : tableInfo->fields)
                            {
                                values.emplace_back(data.second->getField(columnName));
                            }
                            values.emplace_back(to_string(data.second->getStatus()));
                            values.emplace_back(to_string(data.second->num()));
                            stringstream ss;
                            boost::archive::binary_oarchive oa(ss);
                            oa << values;
                            auto realValue = ss.str();
                            {
                                tbb::spin_mutex::scoped_lock lock(batchMutex);
                                writeBatch.Put(
                                    Slice(realKey), Slice(realValue.data(), realValue.size()));
                            }
                        }
                        total.fetch_add(1);
                    });
                // TODO: maybe commit table to different column family according second path
            }
        });
    auto serialization_time_cost = utcTime();
    // commit current tableID in meta column family
    auto currentTableID = m_tableID.load() - 1;
    writeBatch.Put(m_metadataCF, Slice(CURRENT_TABLE_ID), Slice(to_string(currentTableID)));
    // commit to rocksDB
    auto status = m_db->Write(WriteOptions(), &writeBatch);
    if (!status.ok())
    {  // panic
        STORAGE_LOG(ERROR) << LOG_BADGE("RocksDBAdapter commitTables failed")
                           << LOG_KV("message", status.ToString());
        return {0, make_shared<Error>(StorageErrorCode::DataBaseUnavailable, status.ToString())};
    }

    STORAGE_LOG(INFO) << LOG_BADGE("RocksDBAdapter") << LOG_DESC("commitTables")
                      << LOG_KV("tables", _tableDatas.size()) << LOG_KV("rows", total.load())
                      << LOG_KV("assignIDTimeCost", assignID_time_cost - start_time)
                      << LOG_KV("encodeTimeCost", serialization_time_cost - assignID_time_cost)
                      << LOG_KV("writeDBTimeCost", utcTime() - serialization_time_cost)
                      << LOG_KV("totalTimeCost", utcTime() - start_time);
    return {total.load(), nullptr};
}

}  // namespace storage
}  // namespace bcos
