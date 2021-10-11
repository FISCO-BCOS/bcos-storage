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
 * @brief the implement of TiKVStorage
 * @file TiKVStorage.h
 * @author: xingqiangbai
 * @date: 2021-09-26
 */
#include "TiKVStorage.h"
#include "Common.h"
#include "bcos-framework/interfaces/storage/Table.h"
#include "bcos-framework/libutilities/Error.h"
#include "interfaces/protocol/ProtocolTypeDef.h"
#include "pingcap/kv/BCOS2PC.h"
#include "pingcap/kv/Scanner.h"
#include "pingcap/kv/Snapshot.h"
#include "pingcap/kv/Txn.h"
#include <tbb/concurrent_vector.h>
#include <tbb/spin_mutex.h>
#include <exception>

using namespace bcos::storage;
using namespace pingcap::kv;
using namespace std;

TiKVStorage::TiKVStorage(const std::shared_ptr<pingcap::kv::Cluster>& _cluster)
  : m_cluster(_cluster)
{
    m_snapshot = std::make_shared<Snapshot>(m_cluster.get());
}

void TiKVStorage::asyncGetPrimaryKeys(const std::string_view& _table,
    const std::optional<Condition const>& _condition,
    std::function<void(Error::UniquePtr&&, std::vector<std::string>&&)> _callback) noexcept
{
    std::vector<std::string> result;

    std::string keyPrefix;
    keyPrefix = string(_table) + TABLE_KEY_SPLIT;

    auto scanner = m_snapshot->Scan(keyPrefix, string());

    // FIXME: check performance and add limit of primary keys
    for (; scanner.valid && scanner.key().rfind(keyPrefix, 0) == 0; scanner.next())
    {
        if (scanner.value().empty())
        {
            continue;
        }
        size_t start = keyPrefix.size();
        auto key = scanner.key().substr(start);
        if (!_condition || _condition->isValid(key))
        {  // filter by condition, remove keyPrefix
            result.push_back(std::move(key));
        }
    }

    _callback(nullptr, std::move(result));
}

void TiKVStorage::asyncGetRow(const std::string_view& _table, const std::string_view& _key,
    std::function<void(Error::UniquePtr&&, std::optional<Entry>&&)> _callback) noexcept
{
    try
    {
        auto dbKey = toDBKey(_table, _key);
        auto value = m_snapshot->Get(dbKey);
        if (value.empty())
        {
            _callback(nullptr, std::optional<Entry>());
            return;
        }
        TableInfo::ConstPtr tableInfo = getTableInfo(_table);
        if (!tableInfo)
        {
            _callback(BCOS_ERROR_UNIQUE_PTR(-1, "asyncGetRow failed because can't get TableInfo!"),
                std::optional<Entry>());
        }
        _callback(nullptr, decodeEntry(tableInfo, value));
    }
    catch (const std::exception& e)
    {
        _callback(
            BCOS_ERROR_WITH_PREV_UNIQUE_PTR(-1, "asyncGetRow failed!", e), std::optional<Entry>());
    }
}

void TiKVStorage::asyncGetRows(const std::string_view& _table,
    const std::variant<const gsl::span<std::string_view const>, const gsl::span<std::string const>>&
        _keys,
    std::function<void(Error::UniquePtr&&, std::vector<std::optional<Entry>>&&)> _callback) noexcept
{
    try
    {  // FIXME: add batch get of tikv client and use it
        std::visit(
            [&](auto const& keys) {
                std::vector<std::optional<Entry>> entries(keys.size());
                TableInfo::ConstPtr tableInfo = getTableInfo(_table);
                if (!tableInfo)
                {
                    _callback(BCOS_ERROR_UNIQUE_PTR(
                                  -1, "asyncGetRows failed because can't get TableInfo!"),
                        std::vector<std::optional<Entry>>());
                }
                tbb::parallel_for(tbb::blocked_range<size_t>(0, keys.size()),
                    [&](const tbb::blocked_range<size_t>& range) {
                        for (size_t i = range.begin(); i != range.end(); ++i)
                        {
                            auto dbKey = toDBKey(_table, keys[i]);
                            auto value = m_snapshot->Get(dbKey);
                            if (!value.empty())
                            {
                                entries[i] = decodeEntry(tableInfo, value);
                            }
                        }
                    });

                _callback(nullptr, std::move(entries));
            },
            _keys);
    }
    catch (const std::exception& e)
    {
        _callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(-1, "asyncGetRows failed! ", e),
            std::vector<std::optional<Entry>>());
    }
}

void TiKVStorage::asyncSetRow(const std::string_view& _table, const std::string_view& _key,
    Entry _entry, std::function<void(Error::UniquePtr&&)> _callback) noexcept
{
    try
    {
        auto dbKey = toDBKey(_table, _key);
        Txn txn(m_cluster.get());

        if (_entry.status() == Entry::DELETED)
        {  // TODO: check delete
            txn.set(dbKey, "");
        }
        else
        {
            std::string value = encodeEntry(_entry);
            txn.set(dbKey, value);
        }
        txn.commit();
        _callback(nullptr);
    }
    catch (const std::exception& e)
    {
        _callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(-1, "asyncSetRow failed! ", e));
    }
}

void TiKVStorage::asyncPrepare(const TwoPCParams& param,
    const TraverseStorageInterface::ConstPtr& storage,
    std::function<void(Error::Ptr&&, uint64_t startTS)> callback) noexcept
{
    try
    {
        std::unordered_map<std::string, std::string> mutations;

        tbb::spin_mutex writeMutex;
        storage->parallelTraverse(true,
            [&](const std::string_view& table, const std::string_view& key, Entry const& entry) {
                auto dbKey = toDBKey(table, key);

                if (entry.status() == Entry::DELETED)
                {
                    tbb::spin_mutex::scoped_lock lock(writeMutex);
                    mutations[dbKey] = "";
                }
                else
                {
                    std::string value = encodeEntry(entry);
                    tbb::spin_mutex::scoped_lock lock(writeMutex);
                    mutations[dbKey] = std::move(value);
                }
                return true;
            });
        auto size = mutations.size();
        auto primaryLock = toDBKey(param.primaryTableName, param.primaryTableKey);
        m_committer = std::make_shared<BCOSTwoPhaseCommitter>(
            m_cluster.get(), primaryLock, std::move(mutations));
        if (param.startTS == 0)
        {
            auto result = m_committer->prewriteKeys();
            STORAGE_LOG(INFO) << "asyncPrepare primary" << LOG_KV("blockNumber", param.number)
                              << LOG_KV("size", size) << LOG_KV("primaryLock", primaryLock)
                              << LOG_KV("startTS", result.start_ts);
            callback(nullptr, result.start_ts);
        }
        else
        {
            m_committer->prewriteKeys(param.startTS);
            STORAGE_LOG(INFO) << "asyncPrepare secondary" << LOG_KV("blockNumber", param.number)
                              << LOG_KV("size", size) << LOG_KV("primaryLock", primaryLock)
                              << LOG_KV("startTS", param.startTS);
            m_committer = nullptr;
            callback(nullptr, 0);
        }
    }
    catch (const std::exception& e)
    {
        callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(-1, "asyncPrepare failed! ", e), 0);
    }
}

void TiKVStorage::asyncCommit(
    const TwoPCParams& params, std::function<void(Error::Ptr&&)> callback) noexcept
{
    std::ignore = params;
    if (m_committer)
    {
        m_committer->commitKeys();
    }
    m_snapshot = std::make_shared<Snapshot>(m_cluster.get());
    callback(nullptr);
}

void TiKVStorage::asyncRollback(
    const TwoPCParams& params, std::function<void(Error::Ptr&&)> callback) noexcept
{  // FIXME: add support of rollback
    std::ignore = params;
    m_committer = nullptr;
    callback(nullptr);
}

TableInfo::ConstPtr TiKVStorage::getTableInfo(const std::string_view& tableName)
{
    std::promise<TableInfo::ConstPtr> prom;
    asyncOpenTable(tableName, [&prom](Error::UniquePtr&& error, std::optional<Table>&& table) {
        if (error || !table)
        {
            STORAGE_LOG(WARNING) << "asyncGetRow get TableInfo failed"
                                 << LOG_KV("message", error->errorMessage());
            prom.set_value(nullptr);
        }
        else
        {
            prom.set_value(table->tableInfo());
        }
    });
    return prom.get_future().get();
}
