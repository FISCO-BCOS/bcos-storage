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
 * @brief interface of AdapterInterface
 * @file AdapterInterface.h
 * @author: xingqiangbai
 * @date: 2021-04-07
 */
#pragma once
#include "bcos-framework/interfaces/storage/Common.h"
#include "libutilities/Error.h"
#include <functional>

namespace bcos
{
namespace storage
{
class AdapterInterface : public std::enable_shared_from_this<AdapterInterface>
{
public:
    using Ptr = std::shared_ptr<AdapterInterface>;
    AdapterInterface() = default;
    virtual ~AdapterInterface() {}
    virtual std::vector<std::string> getPrimaryKeys(
        const TableInfo::Ptr& _tableInfo, const Condition::Ptr& _condition) const = 0;
    virtual Entry::Ptr getRow(
        const TableInfo::Ptr& _tableInfo, const std::string_view& _key) = 0;
    virtual std::map<std::string, Entry::Ptr> getRows(
        const TableInfo::Ptr& _tableInfo, const std::vector<std::string>& _keys) = 0;
    virtual std::pair<size_t, Error::Ptr> commitTables(const std::vector<std::shared_ptr<TableInfo>>& _tableInfos,
        const std::vector<std::shared_ptr<std::map<std::string, Entry::Ptr>>>&
            _tableDatas) = 0;
};

}  // namespace storage
}  // namespace bcos
