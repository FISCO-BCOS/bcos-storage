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
 * @file Storage.cpp
 * @author: ancelmo
 * @date: 2021-08-27
 */

#pragma once

#include "bcos-framework/interfaces/storage/StorageInterface.h"
#include "rocksdb/db.h"

namespace bcos::storage
{
class RocksDBStorage : public TransactionalStorageInterface
{
public:
    using Ptr = std::shared_ptr<RocksDBStorage>;

    explicit RocksDBStorage();
    ~RocksDBStorage() {}

    void asyncGetPrimaryKeys(const TableInfo::Ptr& _tableInfo, const Condition::Ptr& _condition,
        std::function<void(Error::Ptr&&, std::vector<std::string>&&)> _callback) noexcept override;

    void asyncGetRow(const TableInfo::Ptr& _tableInfo, const std::string& _key,
        std::function<void(Error::Ptr&&, Entry::Ptr&&)> _callback) noexcept override;

    void asyncGetRows(const TableInfo::Ptr& _tableInfo, const gsl::span<std::string>& _keys,
        std::function<void(Error::Ptr&&, std::vector<Entry::Ptr>&&)> _callback) noexcept override;

    void asyncSetRow(const TableInfo::Ptr& tableInfo, const std::string& key,
        const Entry::Ptr& entry,
        std::function<void(Error::Ptr&&, bool)> callback) noexcept override;

    void asyncPrepare(const PrepareParams& params, const TraverseStorageInterface::Ptr& storage,
        std::function<void(Error::Ptr&&)> callback) noexcept override;

    void aysncCommit(protocol::BlockNumber blockNumber,
        std::function<void(Error::Ptr&&)> callback) noexcept override;

    void aysncRollback(protocol::BlockNumber blockNumber,
        std::function<void(Error::Ptr&&)> callback) noexcept override;

private:
    std::string toDBKey(const std::string_view& table, const std::string_view& key);

    std::unique_ptr<rocksdb::DB> m_db;
};
}  // namespace bcos::storage
