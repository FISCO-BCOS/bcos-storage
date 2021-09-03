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

#include <bcos-framework/interfaces/storage/StorageInterface.h>
#include <rocksdb/db.h>

namespace bcos::storage
{
class RocksDBStorage : public TransactionalStorageInterface
{
public:
    using Ptr = std::shared_ptr<RocksDBStorage>;
    explicit RocksDBStorage(std::unique_ptr<rocksdb::DB>&& db) : m_db(std::move(db)) {}

    ~RocksDBStorage() {}

    void asyncGetPrimaryKeys(const TableInfo::ConstPtr& _tableInfo,
        const std::optional<Condition const>& _condition,
        std::function<void(Error::Ptr&&, std::vector<std::string>&&)> _callback) noexcept override;

    void asyncGetRow(const TableInfo::ConstPtr& _tableInfo, const std::string& _key,
        std::function<void(Error::Ptr&&, std::optional<Entry>&&)> _callback) noexcept override;

    void asyncGetRows(const TableInfo::ConstPtr& _tableInfo,
        const gsl::span<std::string const>& _keys,
        std::function<void(Error::Ptr&&, std::vector<std::optional<Entry>>&&)> _callback) noexcept
        override;

    void asyncSetRow(const TableInfo::ConstPtr& tableInfo, const std::string& key, Entry entry,
        std::function<void(Error::Ptr&&, bool)> callback) noexcept override;

    void asyncPrepare(const TwoPCParams& params, const TraverseStorageInterface::ConstPtr& storage,
        std::function<void(Error::Ptr&&)> callback) noexcept override;

    void asyncCommit(
        const TwoPCParams& params, std::function<void(Error::Ptr&&)> callback) noexcept override;

    void asyncRollback(
        const TwoPCParams& params, std::function<void(Error::Ptr&&)> callback) noexcept override;

private:
    std::string toDBKey(const TableInfo::ConstPtr& tableInfo, const std::string_view& key);

    std::string encodeEntry(const Entry& entry);
    Entry decodeEntry(const TableInfo::ConstPtr& tableInfo, bcos::protocol::BlockNumber blockNumber,
        const std::string_view& buffer);

    std::unique_ptr<rocksdb::DB> m_db;
};
}  // namespace bcos::storage
