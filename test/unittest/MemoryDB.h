/**
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
 * @brief the AdapterInterface implement in memory
 * @file MemoryDB.h
 */

#pragma once

#include "AdapterInterface.h"
#include <mutex>
#include <string>

namespace bcos
{
namespace storage
{
class MemoryStorage : public AdapterInterface
{
public:
    MemoryStorage() = default;
    virtual ~MemoryStorage() = default;

    std::vector<std::string> getPrimaryKeys(
        std::shared_ptr<TableInfo> _tableInfo, std::shared_ptr<Condition> _condition) const override
    {
        std::vector<std::string> ret;
        std::lock_guard<std::mutex> lock(m_mutex);
        if (data.count(_tableInfo->name))
        {
            for (auto& kv : data.at(_tableInfo->name))
            {
                if (!_condition || _condition->isValid(kv.first))
                {
                    ret.emplace_back(kv.first);
                }
            }
        }
        return ret;
    }
    std::shared_ptr<Entry> getRow(
        std::shared_ptr<TableInfo> _tableInfo, const std::string_view& _key) override
    {
        std::shared_ptr<Entry> ret = nullptr;
        std::lock_guard<std::mutex> lock(m_mutex);
        if (data.count(_tableInfo->name))
        {
            if (data[_tableInfo->name].count(std::string(_key)))
            {
                if (data[_tableInfo->name][std::string(_key)]->getStatus() == Entry::Status::NORMAL)
                {
                    return data[_tableInfo->name][std::string(_key)];
                }
            }
        }
        return ret;
    }
    std::map<std::string, std::shared_ptr<Entry>> getRows(
        std::shared_ptr<TableInfo> _tableInfo, const std::vector<std::string>& _keys) override
    {
        std::map<std::string, std::shared_ptr<Entry>> ret;
        std::lock_guard<std::mutex> lock(m_mutex);
        if (data.count(_tableInfo->name))
        {
            for (auto& key : _keys)
            {
                if (data[_tableInfo->name].count(std::string(key)))
                {
                    if (data[_tableInfo->name][key]->getStatus() == Entry::Status::NORMAL)
                    {  // this if is unnecessary
                        ret[key] = data[_tableInfo->name][key];
                    }
                }
            }
        }
        return ret;
    }
    std::pair<size_t, Error::Ptr> commitTables(
        const std::vector<std::shared_ptr<TableInfo>> _tableInfos,
        std::vector<std::shared_ptr<std::map<std::string, std::shared_ptr<Entry>>>>& _tableDatas)
        override
    {
        size_t total = 0;
        if (_tableInfos.size() != _tableDatas.size())
        {
            return {0, nullptr};
        }
        std::lock_guard<std::mutex> lock(m_mutex);
        for (size_t i = 0; i < _tableInfos.size(); ++i)
        {
            for (auto& item : *_tableDatas[i])
            {
                if (item.second->getStatus() == Entry::Status::NORMAL)
                {
                    data[_tableInfos[i]->name][item.first] = item.second;
                    ++total;
                }
            }
        }
        return {total, nullptr};
    }

private:
    std::map<std::string, std::map<std::string, Entry::Ptr>> data;
    mutable std::mutex m_mutex;
};

}  // namespace storage

}  // namespace bcos
