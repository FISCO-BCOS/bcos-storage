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
 * @brief interface of KVDBImpl
 * @file KVDBImpl.h
 * @author: xingqiangbai
 * @date: 2021-05-12
 */
#pragma once

#include "bcos-storage/KVDBInterface.h"
#include <map>
#include <string>

namespace rocksdb
{
class DB;
}  // namespace rocksdb

namespace bcos
{
namespace storage
{
class KVDBImpl : public KVDBInterface
{
public:
    using Ptr = std::shared_ptr<KVDBImpl>;
    explicit KVDBImpl(rocksdb::DB* _db);
    virtual ~KVDBImpl() = default;
    Error::Ptr put(const std::string_view& _columnFamily, const std::string_view& _key,
        const std::string_view& _value) override;
    std::pair<std::string, Error::Ptr> get(const std::string_view& _columnFamily, const std::string_view& _key) override;
    Error::Ptr remove(const std::string_view& _columnFamily, const std::string_view& _key) override;
    std::shared_ptr<std::vector<std::string> > multiGet(
        const std::string& _columnFamily, std::vector<std::string>& _keys) override;

private:
    std::unique_ptr<rocksdb::DB> m_db;
};

}  // namespace storage
}  // namespace bcos
