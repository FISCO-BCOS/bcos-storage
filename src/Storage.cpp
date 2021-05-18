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
#include "Storage.h"
#include "bcos-framework/interfaces/storage/StorageInterface.h"
#include "bcos-framework/libutilities/ThreadPool.h"
#include "rocksdb/db.h"

using namespace std;
using namespace rocksdb;

namespace bcos
{
namespace storage
{
StorageImpl::StorageImpl(std::shared_ptr<AdapterInterface> _stateDB,
    std::shared_ptr<KVDBInterface> _kvDB, size_t _poolSize)
  : m_stateDB(_stateDB), m_kvDB(_kvDB)
{
    assert(m_stateDB);
    assert(m_kvDB);
    m_threadPool = std::make_shared<bcos::ThreadPool>("asyncTasks", _poolSize);
}

std::vector<std::string> StorageImpl::getPrimaryKeys(
    std::shared_ptr<TableInfo> _tableInfo, std::shared_ptr<Condition> _condition) const
{
    return m_stateDB->getPrimaryKeys(_tableInfo, _condition);
}

std::shared_ptr<Entry> StorageImpl::getRow(
    std::shared_ptr<TableInfo> _tableInfo, const std::string_view& _key)
{
    return m_stateDB->getRow(_tableInfo, _key);
}

std::map<std::string, std::shared_ptr<Entry>> StorageImpl::getRows(
    std::shared_ptr<TableInfo> _tableInfo, const std::vector<std::string>& _keys)
{
    return m_stateDB->getRows(_tableInfo, _keys);
}

size_t StorageImpl::commitBlock(protocol::BlockNumber _number,
    const std::vector<std::shared_ptr<TableInfo>> _infos,
    std::vector<std::shared_ptr<std::map<std::string, std::shared_ptr<Entry>>>>& _datas)
{
    // TODO: merge state cache then commit
    (void)_number;
    return m_stateDB->commitTables(_infos, _datas);
}

void StorageImpl::asyncGetPrimaryKeys(std::shared_ptr<TableInfo> _tableInfo,
    std::shared_ptr<Condition> _condition,
    std::function<void(Error::Ptr, std::vector<std::string>)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));
    m_threadPool->enqueue([_tableInfo, _condition, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            auto ret = storage->getPrimaryKeys(_tableInfo, _condition);
            _callback(make_shared<Error>(), ret);
        }
        else
        {
            _callback(make_shared<Error>(StorageInterface::ErrorCode::DataBase_Unavailable,
                          "database is unavailable"),
                std::vector<std::string>());
        }
    });
}

void StorageImpl::asyncGetRow(std::shared_ptr<TableInfo> _tableInfo,
    std::shared_ptr<std::string> _key,
    std::function<void(Error::Ptr, std::shared_ptr<Entry>)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));
    m_threadPool->enqueue([_tableInfo, _key, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            auto ret = storage->getRow(_tableInfo, *_key);
            _callback(make_shared<Error>(), ret);
        }
        else
        {
            _callback(make_shared<Error>(StorageInterface::ErrorCode::DataBase_Unavailable,
                          "database is unavailable"),
                nullptr);
        }
    });
}

void StorageImpl::asyncGetRows(std::shared_ptr<TableInfo> _tableInfo,
    std::shared_ptr<std::vector<std::string>> _keys,
    std::function<void(Error::Ptr, std::map<std::string, std::shared_ptr<Entry>>)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));
    m_threadPool->enqueue([_tableInfo, _keys, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            auto ret = storage->getRows(_tableInfo, *_keys);
            _callback(make_shared<Error>(), ret);
        }
        else
        {
            _callback(make_shared<Error>(StorageInterface::ErrorCode::DataBase_Unavailable,
                          "database is unavailable"),
                std::map<std::string, std::shared_ptr<Entry>>());
        }
    });
}

void StorageImpl::asyncCommitBlock(protocol::BlockNumber _blockNumber,
    std::shared_ptr<std::vector<std::shared_ptr<TableInfo>>> _infos,
    std::shared_ptr<std::vector<std::shared_ptr<std::map<std::string, Entry::Ptr>>>>& _datas,
    std::function<void(Error::Ptr, size_t)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));

    m_threadPool->enqueue([_infos, _datas, _blockNumber, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            auto ret = storage->commitBlock(_blockNumber, *_infos, *_datas);
            _callback(make_shared<Error>(), ret);
        }
        else
        {
            _callback(make_shared<Error>(StorageInterface::ErrorCode::DataBase_Unavailable,
                          "database is unavailable"),
                0);
        }
    });
}

void StorageImpl::asyncAddStateCache(protocol::BlockNumber _blockNumber,
    std::shared_ptr<TableFactory> _tablefactory, std::function<void(Error::Ptr)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));

    m_threadPool->enqueue([_blockNumber, _tablefactory, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            storage->addStateCache(_blockNumber, _tablefactory);
            _callback(make_shared<Error>());
        }
        else
        {
            _callback(make_shared<Error>(
                StorageInterface::ErrorCode::DataBase_Unavailable, "database is unavailable"));
        }
    });
}

void StorageImpl::asyncDropStateCache(
    protocol::BlockNumber _blockNumber, std::function<void(Error::Ptr)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));

    m_threadPool->enqueue([_blockNumber, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            storage->dropStateCache(_blockNumber);
            _callback(make_shared<Error>());
        }
        else
        {
            _callback(make_shared<Error>(
                StorageInterface::ErrorCode::DataBase_Unavailable, "database is unavailable"));
        }
    });
}

void StorageImpl::asyncGetStateCache(protocol::BlockNumber _blockNumber,
    std::function<void(Error::Ptr, std::shared_ptr<TableFactory>)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));

    m_threadPool->enqueue([_blockNumber, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            auto ret = storage->getStateCache(_blockNumber);
            _callback(make_shared<Error>(), ret);
        }
        else
        {
            _callback(make_shared<Error>(StorageInterface::ErrorCode::DataBase_Unavailable,
                          "database is unavailable"),
                nullptr);
        }
    });
}

std::shared_ptr<TableFactory> StorageImpl::getStateCache(protocol::BlockNumber _blockNumber)
{
    std::shared_lock lock(m_number2TableFactoryMutex);
    if (m_number2TableFactory.count(_blockNumber))
    {
        return m_number2TableFactory[_blockNumber];
    }
    return nullptr;
}
void StorageImpl::dropStateCache(protocol::BlockNumber _blockNumber)
{
    std::unique_lock lock(m_number2TableFactoryMutex);
    m_number2TableFactory.erase(_blockNumber);
}

void StorageImpl::addStateCache(
    protocol::BlockNumber _blockNumber, std::shared_ptr<TableFactory> _tablefactory)
{
    std::unique_lock lock(m_number2TableFactoryMutex);
    m_number2TableFactory[_blockNumber] = _tablefactory;
}

bool StorageImpl::put(const std::string_view& _columnFamily, const std::string_view& key,
    const std::string_view& value)
{
    return m_kvDB->put(_columnFamily, key, value);
}

std::string StorageImpl::get(const std::string_view& _columnFamily, const std::string_view& _key)
{
    return m_kvDB->get(_columnFamily, _key);
}

bool StorageImpl::remove(const std::string_view& _columnFamily, const std::string_view& _key)
{
    return m_kvDB->remove(_columnFamily, _key);
}

void StorageImpl::asyncPut(std::shared_ptr<std::string> _columnFamily,
    std::shared_ptr<std::string> _key, std::shared_ptr<std::string> _value,
    std::function<void(Error::Ptr)> _callback)
{
    auto db = std::weak_ptr<KVDBInterface>(m_kvDB);
    m_threadPool->enqueue([_columnFamily, _key, _value, _callback, db]() {
        auto kvDB = db.lock();
        if (kvDB)
        {
            auto ret = kvDB->put(*_columnFamily, *_key, *_value);
            if (ret)
            {
                _callback(make_shared<Error>());
            }
            else
            {
            }
        }
        else
        {
            _callback(make_shared<Error>(
                StorageInterface::ErrorCode::DataBase_Unavailable, "database is unavailable"));
        }
    });
}

void StorageImpl::asyncGet(std::shared_ptr<std::string> _columnFamily,
    std::shared_ptr<std::string> _key,
    std::function<void(Error::Ptr, const std::string& value)> _callback)
{
    auto db = std::weak_ptr<KVDBInterface>(m_kvDB);
    m_threadPool->enqueue([_columnFamily, _key, _callback, db]() {
        auto kvDB = db.lock();
        if (kvDB)
        {
            auto ret = kvDB->get(*_columnFamily, *_key);
            _callback(make_shared<Error>(), ret);
        }
        else
        {
            _callback(make_shared<Error>(StorageInterface::ErrorCode::DataBase_Unavailable,
                          "database is unavailable"),
                "");
        }
    });
}

void StorageImpl::asyncRemove(std::shared_ptr<std::string> _columnFamily,
    std::shared_ptr<std::string> _key, std::function<void(Error::Ptr)> _callback)
{
    auto db = std::weak_ptr<KVDBInterface>(m_kvDB);
    m_threadPool->enqueue([_columnFamily, _key, _callback, db]() {
        auto kvDB = db.lock();
        if (kvDB)
        {
            auto ret = kvDB->remove(*_columnFamily, *_key);
            if (ret)
            {
                _callback(make_shared<Error>());
            }
            else
            {
            }
        }
        else
        {
            _callback(make_shared<Error>(
                StorageInterface::ErrorCode::DataBase_Unavailable, "database is unavailable"));
        }
    });
}

void StorageImpl::asyncGetBatch(std::shared_ptr<std::string> _columnFamily,
    std::shared_ptr<std::vector<std::string>> _keys,
    std::function<void(Error::Ptr, std::shared_ptr<std::vector<std::string>>)> _callback)
{
    auto db = std::weak_ptr<KVDBInterface>(m_kvDB);
    m_threadPool->enqueue([_columnFamily, _keys, _callback, db]() {
        auto kvDB = db.lock();
        if (kvDB)
        {
            auto values = kvDB->multiGet(*_columnFamily, *_keys);
            _callback(make_shared<Error>(), values);
        }
        else
        {
            _callback(make_shared<Error>(StorageInterface::ErrorCode::DataBase_Unavailable,
                          "database is unavailable"),
                nullptr);
        }
    });
}

}  // namespace storage
}  // namespace bcos
