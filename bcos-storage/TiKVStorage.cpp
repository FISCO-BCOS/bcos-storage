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
#include "pingcap/kv/Cluster.h"
#include "pingcap/kv/Scanner.h"
#include "pingcap/kv/Snapshot.h"
#include "pingcap/kv/Txn.h"
#include <tbb/concurrent_vector.h>
#include <tbb/spin_mutex.h>
#include <exception>

using namespace bcos::storage;
using namespace pingcap::kv;
using namespace std;

#define STORAGE_TIKV_LOG(LEVEL) BCOS_LOG(LEVEL) << "[STORAGE-TiKV]"
namespace bcos::storage
{
std::shared_ptr<pingcap::kv::Cluster> newTiKVCluster(const std::vector<std::string>& pdAddrs)
{
    pingcap::ClusterConfig config;
    // TODO: why config this?
    config.tiflash_engine_key = "engine";
    config.tiflash_engine_value = "tiflash";
    return std::make_shared<Cluster>(pdAddrs, config);
}
}  // namespace bcos::storage

void TiKVStorage::asyncGetPrimaryKeys(const std::string_view& _table,
    const std::optional<Condition const>& _condition,
    std::function<void(Error::UniquePtr, std::vector<std::string>)> _callback) noexcept
{
    auto start = utcTime();
    std::vector<std::string> result;

    std::string keyPrefix;
    keyPrefix = string(_table) + TABLE_KEY_SPLIT;
    auto snap = Snapshot(m_cluster.get());
    auto scanner = snap.Scan(keyPrefix, string());

    // FIXME: check performance and add limit of primary keys
    for (; scanner.valid && scanner.key().rfind(keyPrefix, 0) == 0; scanner.next())
    {
        size_t start = keyPrefix.size();
        auto key = scanner.key().substr(start);
        if (!_condition || _condition->isValid(key))
        {  // filter by condition, remove keyPrefix
            result.push_back(std::move(key));
        }
    }
    auto end = utcTime();
    _callback(nullptr, std::move(result));
    STORAGE_TIKV_LOG(DEBUG) << LOG_DESC("asyncGetPrimaryKeys") << LOG_KV("table", _table)
                            << LOG_KV("count", result.size())
                            << LOG_KV("read time(ms)", end - start)
                            << LOG_KV("callback time(ms)", utcTime() - end);
}

void TiKVStorage::asyncGetRow(const std::string_view& _table, const std::string_view& _key,
    std::function<void(Error::UniquePtr, std::optional<Entry>)> _callback) noexcept
{
    try
    {
        auto start = utcTime();
        auto dbKey = toDBKey(_table, _key);
        auto snap = Snapshot(m_cluster.get());
        auto value = snap.Get(dbKey);
        auto end = utcTime();
        if (value.empty())
        {
            STORAGE_TIKV_LOG(TRACE) << LOG_DESC("asyncGetRow empty") << LOG_KV("table", _table)
                                    << LOG_KV("key", _key) << LOG_KV("dbKey", dbKey);
            _callback(nullptr, {});
            return;
        }
        TableInfo::ConstPtr tableInfo = getTableInfo(_table);
        if (!tableInfo)
        {
            STORAGE_TIKV_LOG(ERROR) << LOG_DESC("asyncGetRow can't get tableInfo")
                                    << LOG_KV("table", _table) << LOG_KV("key", _key);
            _callback(BCOS_ERROR_UNIQUE_PTR(
                          TableNotExists, "asyncGetRow failed because can't get TableInfo!"),
                {});
            return;
        }
        auto end2 = utcTime();
        _callback(nullptr, decodeEntry(tableInfo, value));
        STORAGE_TIKV_LOG(DEBUG) << LOG_DESC("asyncGetRow") << LOG_KV("table", _table)
                                << LOG_KV("key", _key) << LOG_KV("read time(ms)", end - start)
                                << LOG_KV("tableInfo time(ms)", end2 - end)
                                << LOG_KV("callback time(ms)", utcTime() - end2);
    }
    catch (const std::exception& e)
    {
        _callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(UnknownEntryType, "asyncGetRow failed!", e), {});
    }
}

void TiKVStorage::asyncGetRows(const std::string_view& _table,
    const std::variant<const gsl::span<std::string_view const>, const gsl::span<std::string const>>&
        _keys,
    std::function<void(Error::UniquePtr, std::vector<std::optional<Entry>>)> _callback) noexcept
{
    try
    {
        auto start = utcTime();
        std::visit(
            [&](auto const& keys) {
                std::vector<std::optional<Entry>> entries(keys.size());
                TableInfo::ConstPtr tableInfo = getTableInfo(_table);
                if (!tableInfo)
                {
                    STORAGE_TIKV_LOG(ERROR)
                        << LOG_DESC("asyncGetRows failed table doesn't exist")
                        << LOG_KV("table", _table) << LOG_KV("count", keys.size());
                    _callback(BCOS_ERROR_UNIQUE_PTR(TableNotExists,
                                  "asyncGetRows failed because can't get TableInfo!"),
                        std::vector<std::optional<Entry>>());
                    return;
                }

                std::vector<std::string> realKeys(keys.size());
                tbb::parallel_for(tbb::blocked_range<size_t>(0, keys.size()),
                    [&](const tbb::blocked_range<size_t>& range) {
                        for (size_t i = range.begin(); i != range.end(); ++i)
                        {
                            realKeys[i] = toDBKey(_table, keys[i]);
                        }
                    });
                auto snap = Snapshot(m_cluster.get());
                auto result = snap.BatchGet(realKeys);
                auto end = utcTime();

                tbb::parallel_for(tbb::blocked_range<size_t>(0, keys.size()),
                    [&](const tbb::blocked_range<size_t>& range) {
                        for (size_t i = range.begin(); i != range.end(); ++i)
                        {
                            auto value = result[realKeys[i]];
                            if (!value.empty())
                            {
                                entries[i] = decodeEntry(tableInfo, value);
                            }
                        }
                    });
                auto decode = utcTime();
                _callback(nullptr, std::move(entries));
                STORAGE_TIKV_LOG(DEBUG)
                    << LOG_DESC("asyncGetRows") << LOG_KV("table", _table)
                    << LOG_KV("count", entries.size()) << LOG_KV("read time(ms)", end - start)
                    << LOG_KV("decode time(ms)", decode - end)
                    << LOG_KV("callback time(ms)", utcTime() - decode);
            },
            _keys);
    }
    catch (const std::exception& e)
    {
        STORAGE_TIKV_LOG(DEBUG) << LOG_DESC("asyncGetRows failed") << LOG_KV("table", _table)
                                << LOG_KV("message", e.what());
        _callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(UnknownEntryType, "asyncGetRows failed! ", e),
            std::vector<std::optional<Entry>>());
    }
}

void TiKVStorage::asyncSetRow(const std::string_view& _table, const std::string_view& _key,
    Entry _entry, std::function<void(Error::UniquePtr)> _callback) noexcept
{
    try
    {
        auto dbKey = toDBKey(_table, _key);
        Txn txn(m_cluster.get());

        if (_entry.status() == Entry::DELETED)
        {
            STORAGE_TIKV_LOG(DEBUG)
                << LOG_DESC("asyncSetRow delete") << LOG_KV("table", _table) << LOG_KV("key", _key);
            txn.set(dbKey, "");
        }
        else
        {
            STORAGE_TIKV_LOG(DEBUG)
                << LOG_DESC("asyncSetRow") << LOG_KV("table", _table) << LOG_KV("key", _key);
            std::string value = encodeEntry(_entry);
            txn.set(dbKey, value);
        }
        txn.commit();
        _callback(nullptr);
    }
    catch (const std::exception& e)
    {
        _callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(UnknownEntryType, "asyncSetRow failed! ", e));
    }
}

void TiKVStorage::asyncPrepare(const TwoPCParams& param,
    const TraverseStorageInterface::ConstPtr& storage,
    std::function<void(Error::Ptr, uint64_t startTS)> callback) noexcept
{
    try
    {
        auto start = utcTime();
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
        auto encode = utcTime();
        if (mutations.empty() && param.startTS == 0)
        {
            STORAGE_TIKV_LOG(ERROR)
                << LOG_DESC("asyncPrepare empty storage") << LOG_KV("number", param.number);
            callback(BCOS_ERROR_UNIQUE_PTR(EmptyStorage, "commit storage is empty"), 0);
            return;
        }
        auto size = mutations.size();
        auto primaryLock = toDBKey(param.primaryTableName, param.primaryTableKey);
        m_committer = std::make_shared<BCOSTwoPhaseCommitter>(
            m_cluster.get(), primaryLock, std::move(mutations));
        if (param.startTS == 0)
        {
            auto result = m_committer->prewriteKeys();
            auto write = utcTime();
            callback(nullptr, result.start_ts);
            STORAGE_LOG(INFO) << "asyncPrepare primary" << LOG_KV("blockNumber", param.number)
                              << LOG_KV("size", size) << LOG_KV("primaryLock", primaryLock)
                              << LOG_KV("startTS", result.start_ts)
                              << LOG_KV("encode time(ms)", encode - start)
                              << LOG_KV("prewrite time(ms)", write - encode)
                              << LOG_KV("callback time(ms)", utcTime() - write);
        }
        else
        {
            m_committer->prewriteKeys(param.startTS);
            auto write = utcTime();
            m_committer = nullptr;
            callback(nullptr, 0);
            STORAGE_LOG(INFO) << "asyncPrepare secondary" << LOG_KV("blockNumber", param.number)
                              << LOG_KV("size", size) << LOG_KV("primaryLock", primaryLock)
                              << LOG_KV("startTS", param.startTS)
                              << LOG_KV("encode time(ms)", encode - start)
                              << LOG_KV("prewrite time(ms)", write - encode)
                              << LOG_KV("callback time(ms)", utcTime() - write);
        }
    }
    catch (const std::exception& e)
    {
        callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(UnknownEntryType, "asyncPrepare failed! ", e), 0);
    }
}

void TiKVStorage::asyncCommit(
    const TwoPCParams& params, std::function<void(Error::Ptr)> callback) noexcept
{
    auto start = utcTime();
    std::ignore = params;
    if (m_committer)
    {
        m_committer->commitKeys();
        m_committer = nullptr;
    }
    auto end = utcTime();
    callback(nullptr);
    STORAGE_TIKV_LOG(INFO) << LOG_DESC("asyncCommit") << LOG_KV("number", params.number)
                           << LOG_KV("startTS", params.startTS) << LOG_KV("time(ms)", end - start)
                           << LOG_KV("callback time(ms)", utcTime() - end);
}

void TiKVStorage::asyncRollback(
    const TwoPCParams& params, std::function<void(Error::Ptr)> callback) noexcept
{
    auto start = utcTime();
    std::ignore = params;
    if (m_committer)
    {
        m_committer->rollback();
        m_committer = nullptr;
    }
    auto end = utcTime();
    callback(nullptr);
    STORAGE_TIKV_LOG(INFO) << LOG_DESC("asyncRollback") << LOG_KV("number", params.number)
                           << LOG_KV("startTS", params.startTS) << LOG_KV("time(ms)", end - start)
                           << LOG_KV("callback time(ms)", utcTime() - end);
}
