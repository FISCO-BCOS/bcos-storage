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
#pragma once
#include "bcos-framework/interfaces/storage/StorageInterface.h"

namespace bcos
{
namespace storage
{
class StorageImpl : public class StorageInterface
{
public:
    using Ptr = std::shared_ptr<StorageImpl>;
    StorageImpl() = default;
    ~StorageImpl() {}
    std::vector<std::string> getPrimaryKeys(std::shared_ptr<TableInfo> _tableInfo, std::shared_ptr<Condition> _condition) const override;
    std::shared_ptr<Entry> getRow(
        std::shared_ptr<TableInfo>& _tableInfo, const std::string_view& _key) override
    std::map<std::string, std::shared_ptr<Entry>> getRows(
        std::shared_ptr<TableInfo>& _tableInfo, const std::vector<std::string>& _keys) override;
    size_t commitTables(
        const std::map<std::shared_ptr<TableInfo>, std::shared_ptr<std::map<std::string, std::shared_ptr<Entry>>>>& _data) override;

    void asyncGetPrimaryKeys(std::shared_ptr<TableInfo>& _tableInfo, std::shared_ptr<Condition> _condition,
        std::function<void(Error, std::vector<std::string>)> _callback) const override;
    void asyncGetRow(std::shared_ptr<TableInfo>& _tableInfo, const std::string_view& _key,
        std::function<void(Error, std::shared_ptr<Entry>)> _callback) override;
    void asyncGetRows(std::shared_ptr<TableInfo>& _tableInfo,
        const std::vector<std::string>& _keys,
        std::function<void(Error, std::map<std::string, std::shared_ptr<Entry>>)> _callback) override;
    void asyncCommitTables(
        const std::map<std::shared_ptr<TableInfo>, std::shared_ptr<std::map<std::string, std::shared_ptr<Entry>>>>& _data,
        std::function<void(Error)> _callback) override;

    // cache TableFactory
    void asyncAddStateCache(int64_t _blockNumber, protocol::Block::Ptr _block,
        std::shared_ptr<TableFactory> _tablefactory, std::function<void(Error)> _callback) override;
    bool asyncDropStateCache(
        int64_t _blockNumber, std::function<void(Error)> _callback) override;
    void asyncGetBlock(
        int64_t _blockNumber, std::function<void(Error, protocol::Block::Ptr)> _callback) override;
    void asyncGetStateCache(int64_t _blockNumber,
        std::function<void(Error, std::shared_ptr<TableFactory>)> _callback) override;
    protocol::Block::Ptr getBlock(int64_t _blockNumber) override;
    std::shared_ptr<TableFactory> getStateCache(int64_t _blockNumber) override;
protected:

};
}  // namespace storage
}  // namespace bcos
