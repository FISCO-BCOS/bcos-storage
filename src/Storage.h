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
 * @brief the implement of storage
 * @file Storage.cpp
 * @author: xingqiangbai
 * @date: 2021-04-16
 */

#pragma once

#include "AdapterInterface.h"
#include "KVDBInterface.h"
#include "bcos-framework/interfaces/storage/StorageInterface.h"
#include <shared_mutex>

namespace bcos
{
class ThreadPool;
namespace storage
{
class StorageImpl : public StorageInterface
{
public:
    using Ptr = std::shared_ptr<StorageImpl>;
    explicit StorageImpl(const std::shared_ptr<AdapterInterface> _stateDB,
        const std::shared_ptr<KVDBInterface> _kvDB, size_t _poolSize = 4);
    ~StorageImpl() {}
    std::vector<std::string> getPrimaryKeys(
        const TableInfo::Ptr& _tableInfo, const Condition::Ptr& _condition) const override;
    Entry::Ptr getRow(const TableInfo::Ptr& _tableInfo, const std::string_view& _key) override;
    std::map<std::string, Entry::Ptr> getRows(
        const TableInfo::Ptr& _tableInfo, const std::vector<std::string>& _keys) override;
    std::pair<size_t, Error::Ptr> commitBlock(protocol::BlockNumber _number,
        const std::vector<TableInfo::Ptr>& _infos,
        const std::vector<std::shared_ptr<std::map<std::string, Entry::Ptr>>>& _datas) override;

    void asyncGetPrimaryKeys(const TableInfo::Ptr& _tableInfo, const Condition::Ptr& _condition,
        std::function<void(const Error::Ptr&, const std::vector<std::string>&)> _callback) override;
    void asyncGetRow(const TableInfo::Ptr& _tableInfo, const std::string_view& _key,
        std::function<void(const Error::Ptr&, const Entry::Ptr&)> _callback) override;
    void asyncGetRows(const TableInfo::Ptr& _tableInfo,
        const std::shared_ptr<std::vector<std::string>>& _keys,
        std::function<void(const Error::Ptr&, const std::map<std::string, Entry::Ptr>&)> _callback)
        override;
    void asyncCommitBlock(protocol::BlockNumber _blockNumber,
        const std::shared_ptr<std::vector<TableInfo::Ptr>>& _infos,
        const std::shared_ptr<std::vector<std::shared_ptr<std::map<std::string, Entry::Ptr>>>>&
            _datas,
        std::function<void(const Error::Ptr&, size_t)> _callback) override;

    // cache TableFactory
    void asyncAddStateCache(protocol::BlockNumber _blockNumber,
        const std::shared_ptr<TableFactoryInterface>& _tablefactory,
        std::function<void(const Error::Ptr&)> _callback) override;
    void asyncDropStateCache(protocol::BlockNumber _blockNumber,
        std::function<void(const Error::Ptr&)> _callback) override;
    void asyncGetStateCache(protocol::BlockNumber _blockNumber,
        std::function<void(const Error::Ptr&, const std::shared_ptr<TableFactoryInterface>&)>
            _callback) override;
    std::shared_ptr<TableFactoryInterface> getStateCache(
        protocol::BlockNumber _blockNumber) override;
    void dropStateCache(protocol::BlockNumber _blockNumber) override;
    void addStateCache(protocol::BlockNumber _blockNumber,
        const std::shared_ptr<TableFactoryInterface>& _tablefactory) override;

    // KV store in split database, used to store data off-chain
    Error::Ptr put(const std::string_view& _columnFamily, const std::string_view& key,
        const std::string_view& value) override;
    std::pair<std::string, Error::Ptr> get(
        const std::string_view& _columnFamily, const std::string_view& key) override;
    Error::Ptr remove(const std::string_view& _columnFamily, const std::string_view& _key) override;
    void asyncPut(const std::string_view& _columnFamily, const std::string_view& _key,
        const std::string_view& value, std::function<void(const Error::Ptr&)> _callback) override;
    void asyncGet(const std::string_view& _columnFamily, const std::string_view& _key,
        std::function<void(const Error::Ptr&, const std::string& value)> _callback) override;
    void asyncRemove(const std::string_view& _columnFamily, const std::string_view& _key,
        std::function<void(const Error::Ptr&)> _callback) override;
    void asyncGetBatch(const std::string_view& _columnFamily,
        const std::shared_ptr<std::vector<std::string>>& _keys,
        std::function<void(const Error::Ptr&, const std::shared_ptr<std::vector<std::string>>&)> _callback)
        override;

protected:
    std::shared_ptr<AdapterInterface> m_stateDB = nullptr;
    std::shared_ptr<KVDBInterface> m_kvDB = nullptr;
    std::shared_ptr<ThreadPool> m_threadPool = nullptr;
    mutable std::shared_mutex m_number2TableFactoryMutex;
    std::map<protocol::BlockNumber, std::shared_ptr<TableFactoryInterface>> m_number2TableFactory;
};
}  // namespace storage
}  // namespace bcos
