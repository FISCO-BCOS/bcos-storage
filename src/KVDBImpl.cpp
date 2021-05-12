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
 * @brief use rocksDB implement of KVDBInterface
 * @file KVDBImpl.cpp
 * @author: xingqiangbai
 * @date: 2021-05-12
 */

#include "KVDBImpl.h"
#include "bcos-framework/interfaces/storage/Common.h"
#include "boost/filesystem.hpp"
#include "rocksdb/db.h"

using namespace std;
using namespace rocksdb;

namespace bcos
{
namespace storage
{
KVDBImpl::KVDBImpl(rocksdb::DB* _db) : m_db(_db) {}

bool KVDBImpl::put(
    const std::string& columnFamily, const std::string_view& key, const std::string_view& value)
{
    string realeKey = columnFamily + "_" + string(key);
    auto status = m_db->Put(
        WriteOptions(), Slice(realeKey.data(), realeKey.size()), Slice(value.data(), value.size()));
    if (!status.ok())
    {
        STORAGE_LOG(ERROR) << LOG_BADGE("KVDBImpl put failed") << LOG_KV("key", key)
                           << LOG_KV("message", status.ToString());
    }
    return status.ok();
}

std::string KVDBImpl::get(const std::string& columnFamily, const std::string_view& key)
{
    string value;
    string realeKey = columnFamily + "_" + string(key);
    auto status = m_db->Get(ReadOptions(), Slice(realeKey.data(), realeKey.size()), &value);
    if (!status.ok())
    {
        STORAGE_LOG(ERROR) << LOG_BADGE("KVDBImpl get failed") << LOG_KV("key", key)
                           << LOG_KV("message", status.ToString());
    }
    return value;
}

std::shared_ptr<std::vector<std::string>> KVDBImpl::multiGet(
    const std::string& _columnFamily, std::vector<std::string_view>& _keys)
{
    vector<string> realkeys(_keys.size(), _columnFamily);
    vector<Slice> keys;
    keys.reserve(_keys.size());
    for (size_t i = 0; i < _keys.size(); ++i)
    {
        realkeys[i].append("_").append(_keys[i].data(), _keys[i].size());
        keys.emplace_back(realkeys[i].data(), realkeys[i].size());
    }
    auto values = make_shared<vector<string>>();
    auto status = m_db->MultiGet(ReadOptions(), keys, values.get());
    for (size_t i = 0; i < _keys.size(); ++i)
    {
        if (!status[i].ok())
        {
            STORAGE_LOG(ERROR) << LOG_BADGE("KVDBImpl multiGet failed")
                               << LOG_KV("message", status[i].ToString())
                               << LOG_KV("first key", _columnFamily)
                               << LOG_KV("second key", _keys[i]);
        }
    }
    return values;
}

}  // namespace storage
}  // namespace bcos
