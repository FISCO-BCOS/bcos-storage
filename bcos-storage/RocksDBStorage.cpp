/*
 *  Copyright (C) 2021 FISCO BCOS.
 *  SPDX-License-Identifier: Apache-2.0
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 * @brief the header of storage
 * @file Storage.h
 * @author: xingqiangbai
 * @date: 2021-04-16
 */
#include "RocksDBStorage.h"
#include "Common.h"
#include "bcos-framework/interfaces/storage/Table.h"
#include "bcos-framework/libutilities/Error.h"
#include "interfaces/protocol/ProtocolTypeDef.h"
#include <rocksdb/cleanable.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <tbb/concurrent_vector.h>
#include <tbb/spin_mutex.h>
#include <exception>
#include <future>

using namespace bcos::storage;
using namespace rocksdb;
using namespace std;

RocksDBStorage::RocksDBStorage(std::unique_ptr<rocksdb::DB>&& db) : m_db(std::move(db))
{
    m_writeBatch = std::make_shared<WriteBatch>();
}

void RocksDBStorage::asyncGetPrimaryKeys(const std::string_view& _table,
    const std::optional<Condition const>& _condition,
    std::function<void(Error::UniquePtr&&, std::vector<std::string>&&)> _callback) noexcept
{
    std::vector<std::string> result;

    std::string keyPrefix;
    keyPrefix = string(_table) + TABLE_KEY_SPLIT;

    ReadOptions read_options;
    read_options.total_order_seek = true;
    auto iter = m_db->NewIterator(read_options);

    // FIXME: check performance and add limit of primary keys
    for (iter->Seek(keyPrefix); iter->Valid() && iter->key().starts_with(keyPrefix); iter->Next())
    {
        size_t start = keyPrefix.size();
        if (!_condition || _condition->isValid(std::string_view(
                               iter->key().data() + start, iter->key().size() - start)))
        {  // filter by condition, the key need
           // remove TABLE_PREFIX
            result.emplace_back(iter->key().ToString().substr(start));
        }
    }
    delete iter;

    _callback(nullptr, std::move(result));
}

void RocksDBStorage::asyncGetRow(const std::string_view& _table, const std::string_view& _key,
    std::function<void(Error::UniquePtr&&, std::optional<Entry>&&)> _callback) noexcept
{
    try
    {
        PinnableSlice value;
        auto dbKey = toDBKey(_table, _key);

        auto status = m_db->Get(
            ReadOptions(), m_db->DefaultColumnFamily(), Slice(dbKey.data(), dbKey.size()), &value);

        if (!status.ok())
        {
            if (status.IsNotFound())
            {
                _callback(nullptr, std::optional<Entry>());
                return;
            }

            std::string errorMessage =
                "RocksDB get failed!, " + boost::lexical_cast<std::string>(status.subcode());
            if (status.getState())
            {
                errorMessage.append(" ").append(status.getState());
            }
            _callback(BCOS_ERROR_UNIQUE_PTR(-1, errorMessage), std::optional<Entry>());

            return;
        }
        TableInfo::ConstPtr tableInfo = getTableInfo(_table);
        if (!tableInfo)
        {
            _callback(BCOS_ERROR_UNIQUE_PTR(-1, "asyncGetRow failed because can't get TableInfo!"),
                std::optional<Entry>());
            return;
        }
        _callback(nullptr, decodeEntry(tableInfo, value.ToStringView()));
    }
    catch (const std::exception& e)
    {
        _callback(
            BCOS_ERROR_WITH_PREV_UNIQUE_PTR(-1, "Get row failed!", e), std::optional<Entry>());
    }
}

void RocksDBStorage::asyncGetRows(const std::string_view& _table,
    const std::variant<const gsl::span<std::string_view const>, const gsl::span<std::string const>>&
        _keys,
    std::function<void(Error::UniquePtr&&, std::vector<std::optional<Entry>>&&)> _callback) noexcept
{
    try
    {
        std::visit(
            [&](auto const& keys) {
                std::vector<std::string> dbKeys(keys.size());
                std::vector<Slice> slices(keys.size());
                tbb::parallel_for(tbb::blocked_range<size_t>(0, keys.size()),
                    [&](const tbb::blocked_range<size_t>& range) {
                        for (size_t i = range.begin(); i != range.end(); ++i)
                        {
                            dbKeys[i] = toDBKey(_table, keys[i]);
                            slices[i] = Slice(dbKeys[i].data(), dbKeys[i].size());
                        }
                    });

                std::vector<PinnableSlice> values(keys.size());
                std::vector<Status> statusList(keys.size());
                m_db->MultiGet(ReadOptions(), m_db->DefaultColumnFamily(), slices.size(),
                    slices.data(), values.data(), statusList.data());

                std::vector<std::optional<Entry>> entries(keys.size());
                TableInfo::ConstPtr tableInfo = getTableInfo(_table);
                if (!tableInfo)
                {
                    _callback(BCOS_ERROR_UNIQUE_PTR(
                                  -1, "asyncGetRows failed because can't get TableInfo!"),
                        std::vector<std::optional<Entry>>());
                    return;
                }
                tbb::parallel_for(tbb::blocked_range<size_t>(0, keys.size()),
                    [&](const tbb::blocked_range<size_t>& range) {
                        for (size_t i = range.begin(); i != range.end(); ++i)
                        {
                            auto& status = statusList[i];
                            auto& value = values[i];

                            if (status.ok())
                            {
                                entries[i] = decodeEntry(tableInfo, value.ToStringView());
                            }
                            else
                            {
                                if (status.IsNotFound())
                                {
                                    STORAGE_LOG(WARNING)
                                        << "Multi get rows, not found key: " << keys[i];
                                }
                                else if (status.getState())
                                {
                                    STORAGE_LOG(WARNING)
                                        << "Multi get rows error: " << status.getState();
                                }
                                else
                                {
                                    STORAGE_LOG(WARNING) << "Multi get rows unknown error";
                                }
                            }
                        }
                    });
                _callback(nullptr, std::move(entries));
            },
            _keys);
    }
    catch (const std::exception& e)
    {
        _callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(-1, "Get rows failed! ", e),
            std::vector<std::optional<Entry>>());
    }
}

void RocksDBStorage::asyncSetRow(const std::string_view& _table, const std::string_view& _key,
    Entry _entry, std::function<void(Error::UniquePtr&&)> _callback) noexcept
{
    try
    {
        auto dbKey = toDBKey(_table, _key);
        WriteOptions options;
        rocksdb::Status status;
        if (_entry.status() == Entry::DELETED)
        {
            status = m_db->Delete(options, dbKey);
        }
        else
        {
            std::string value = encodeEntry(_entry);
            status = m_db->Put(options, dbKey, value);
        }

        if (!status.ok())
        {
            std::string errorMessage = "Set row failed!";
            if (status.getState())
            {
                errorMessage.append(" ").append(status.getState());
            }
            _callback(BCOS_ERROR_UNIQUE_PTR(-1, errorMessage));
            return;
        }

        _callback(nullptr);
    }
    catch (const std::exception& e)
    {
        _callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(-1, "Set row failed! ", e));
    }
}

void RocksDBStorage::asyncPrepare(const TwoPCParams& param,
    const TraverseStorageInterface::ConstPtr& storage,
    std::function<void(Error::Ptr&&, uint64_t startTS)> callback) noexcept
{
    std::ignore = param;
    try
    {
        {
            tbb::spin_mutex::scoped_lock lock(m_writeBatchMutex);
            if (!m_writeBatch)
            {
                m_writeBatch = std::make_shared<WriteBatch>();
            }
        }
        storage->parallelTraverse(true,
            [&](const std::string_view& table, const std::string_view& key, Entry const& entry) {
                auto dbKey = toDBKey(table, key);

                if (entry.status() == Entry::DELETED)
                {
                    tbb::spin_mutex::scoped_lock lock(m_writeBatchMutex);
                    m_writeBatch->Delete(dbKey);
                }
                else
                {
                    std::string value = encodeEntry(entry);
                    tbb::spin_mutex::scoped_lock lock(m_writeBatchMutex);
                    auto status = m_writeBatch->Put(dbKey, value);
                }
                return true;
            });
        callback(nullptr, 0);
    }
    catch (const std::exception& e)
    {
        callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(-1, "Prepare failed! ", e), 0);
    }
}

void RocksDBStorage::asyncCommit(
    const TwoPCParams& params, std::function<void(Error::Ptr&&)> callback) noexcept
{
    std::ignore = params;
    {
        tbb::spin_mutex::scoped_lock lock(m_writeBatchMutex);
        if (m_writeBatch)
        {
            m_db->Write(WriteOptions(), m_writeBatch.get());
            m_writeBatch = nullptr;
        }
    }
    callback(nullptr);
}

void RocksDBStorage::asyncRollback(
    const TwoPCParams& params, std::function<void(Error::Ptr&&)> callback) noexcept
{
    std::ignore = params;
    callback(nullptr);
}

TableInfo::ConstPtr RocksDBStorage::getTableInfo(const std::string_view& tableName)
{  // TODO: move this function to TransactionalStorageInterface
    std::promise<TableInfo::ConstPtr> prom;
    asyncOpenTable(
        tableName, [&prom, &tableName](Error::UniquePtr&& error, std::optional<Table>&& table) {
            if (error)
            {
                STORAGE_LOG(WARNING) << "getTableInfo failed" << LOG_KV("tableName", tableName)
                                     << LOG_KV("message", error->errorMessage());
                prom.set_value(nullptr);
            }
            else if (!table)
            {
                STORAGE_LOG(WARNING)
                    << "getTableInfo failed, table doesn't exist" << LOG_KV("tableName", tableName);
                prom.set_value(nullptr);
            }
            else
            {
                prom.set_value(table->tableInfo());
            }
        });
    return prom.get_future().get();
}
