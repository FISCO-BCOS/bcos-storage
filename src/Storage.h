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
    explicit StorageImpl(std::shared_ptr<AdapterInterface> _stateDB,
        std::shared_ptr<KVDBInterface> _kvDB, size_t _poolSize = 4);
    ~StorageImpl() {}
    std::vector<std::string> getPrimaryKeys(std::shared_ptr<TableInfo> _tableInfo,
        std::shared_ptr<Condition> _condition) const override;
    std::shared_ptr<Entry> getRow(
        std::shared_ptr<TableInfo> _tableInfo, const std::string_view& _key) override;
    std::map<std::string, std::shared_ptr<Entry>> getRows(
        std::shared_ptr<TableInfo> _tableInfo, const std::vector<std::string>& _keys) override;
    std::pair<size_t, Error::Ptr> commitBlock(protocol::BlockNumber _number,
        const std::vector<std::shared_ptr<TableInfo>> _infos,
        std::vector<std::shared_ptr<std::map<std::string, std::shared_ptr<Entry>>>>& _datas)
        override;

    void asyncGetPrimaryKeys(std::shared_ptr<TableInfo> _tableInfo,
        std::shared_ptr<Condition> _condition,
        std::function<void(Error::Ptr, std::vector<std::string>)> _callback) override;
    void asyncGetRow(std::shared_ptr<TableInfo> _tableInfo, std::shared_ptr<std::string> _key,
        std::function<void(Error::Ptr, std::shared_ptr<Entry>)> _callback) override;
    void asyncGetRows(std::shared_ptr<TableInfo> _tableInfo,
        std::shared_ptr<std::vector<std::string>> _keys,
        std::function<void(Error::Ptr, std::map<std::string, std::shared_ptr<Entry>>)> _callback)
        override;
    void asyncCommitBlock(protocol::BlockNumber _blockNumber,
        std::shared_ptr<std::vector<std::shared_ptr<TableInfo>>> _infos,
        std::shared_ptr<std::vector<std::shared_ptr<std::map<std::string, Entry::Ptr>>>>& _datas,
        std::function<void(Error::Ptr, size_t)> _callback) override;

    // cache TableFactory
    void asyncAddStateCache(protocol::BlockNumber _blockNumber,
        std::shared_ptr<TableFactory> _tablefactory,
        std::function<void(Error::Ptr)> _callback) override;
    void asyncDropStateCache(
        protocol::BlockNumber _blockNumber, std::function<void(Error::Ptr)> _callback) override;
    void asyncGetStateCache(protocol::BlockNumber _blockNumber,
        std::function<void(Error::Ptr, std::shared_ptr<TableFactory>)> _callback) override;
    std::shared_ptr<TableFactory> getStateCache(protocol::BlockNumber _blockNumber) override;
    void dropStateCache(protocol::BlockNumber _blockNumber) override;
    void addStateCache(
        protocol::BlockNumber _blockNumber, std::shared_ptr<TableFactory> _tablefactory) override;

    // KV store in split database, used to store data off-chain
    Error::Ptr put(const std::string_view& _columnFamily, const std::string_view& key,
        const std::string_view& value) override;
    std::pair<std::string, Error::Ptr> get(const std::string_view& _columnFamily, const std::string_view& key) override;
    Error::Ptr remove(const std::string_view& _columnFamily, const std::string_view& _key) override;
    void asyncPut(std::shared_ptr<std::string> _columnFamily, std::shared_ptr<std::string> _key,
        std::shared_ptr<bytes> value, std::function<void(Error::Ptr)> _callback) override;
    void asyncGet(std::shared_ptr<std::string> _columnFamily, std::shared_ptr<std::string> _key,
        std::function<void(Error::Ptr, const std::string& value)> _callback) override;
    void asyncRemove(std::shared_ptr<std::string> _columnFamily, std::shared_ptr<std::string> _key,
        std::function<void(Error::Ptr)> _callback) override;
    void asyncGetBatch(std::shared_ptr<std::string> _columnFamily,
        std::shared_ptr<std::vector<std::string>> _keys,
        std::function<void(Error::Ptr, std::shared_ptr<std::vector<std::string>>)> _callback)
        override;

protected:
    std::shared_ptr<AdapterInterface> m_stateDB = nullptr;
    std::shared_ptr<KVDBInterface> m_kvDB = nullptr;
    std::shared_ptr<ThreadPool> m_threadPool = nullptr;
    mutable std::shared_mutex m_number2TableFactoryMutex;
    std::map<protocol::BlockNumber, std::shared_ptr<TableFactory>> m_number2TableFactory;
};
}  // namespace storage
}  // namespace bcos
