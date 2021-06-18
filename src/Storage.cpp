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
#include "bcos-framework/libtable/TableFactory.h"
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

void StorageImpl::stop()
{
    m_threadPool->stop();
}

std::vector<std::string> StorageImpl::getPrimaryKeys(
    const TableInfo::Ptr& _tableInfo, const Condition::Ptr& _condition) const
{
    return m_stateDB->getPrimaryKeys(_tableInfo, _condition);
}

Entry::Ptr StorageImpl::getRow(const TableInfo::Ptr& _tableInfo, const std::string_view& _key)
{
    return m_stateDB->getRow(_tableInfo, _key);
}

std::map<std::string, Entry::Ptr> StorageImpl::getRows(
    const TableInfo::Ptr& _tableInfo, const std::vector<std::string>& _keys)
{
    return m_stateDB->getRows(_tableInfo, _keys);
}

std::pair<size_t, Error::Ptr> StorageImpl::commitBlock(protocol::BlockNumber _number,
    const std::vector<TableInfo::Ptr>& _infos,
    const std::vector<std::shared_ptr<std::map<std::string, Entry::Ptr>>>& _datas)
{
    STORAGE_LOG(INFO) << LOG_BADGE("StorageImpl") << LOG_DESC("commitBlock")
                      << LOG_KV("block", _number);
    // merge state cache then commit
    std::shared_ptr<TableFactoryInterface> stateTableFactory = nullptr;
    {
        std::shared_lock lock(m_number2TableFactoryMutex);
        if (m_number2TableFactory.count(_number))
        {
            stateTableFactory = m_number2TableFactory.at(_number);
        }
    }
    if (stateTableFactory)
    {
        auto stateData = stateTableFactory->exportData();
        stateData.first.insert(stateData.first.end(), _infos.begin(), _infos.end());
        stateData.second.insert(stateData.second.end(), _datas.begin(), _datas.end());

        auto ret = m_stateDB->commitTables(stateData.first, stateData.second);
        if (!ret.second)
        {  // drop state cache, when commite succeed
            dropStateCache(_number);
        }
        return ret;
    }
    // empty block has not state and consensus will commit empty block
    auto ret = m_stateDB->commitTables(_infos, _datas);
    if (!ret.second)
    {  // drop state cache, when commite succeed
        dropStateCache(_number);
    }
    return ret;
}

void StorageImpl::asyncGetPrimaryKeys(const TableInfo::Ptr& _tableInfo,
    const Condition::Ptr& _condition,
    std::function<void(const Error::Ptr&, const std::vector<std::string>&)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));
    m_threadPool->enqueue([_tableInfo, _condition, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            auto ret = storage->getPrimaryKeys(_tableInfo, _condition);
            _callback(nullptr, ret);
        }
        else
        {
            _callback(make_shared<Error>(
                          StorageErrorCode::DataBaseUnavailable, "database is unavailable"),
                std::vector<std::string>());
        }
    });
}

void StorageImpl::asyncGetRow(const TableInfo::Ptr& _tableInfo, const string_view& _key,
    std::function<void(const Error::Ptr&, const Entry::Ptr&)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));
    m_threadPool->enqueue([_tableInfo, key = string(_key), _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            auto ret = storage->getRow(_tableInfo, key);
            _callback(nullptr, ret);
        }
        else
        {
            _callback(make_shared<Error>(
                          StorageErrorCode::DataBaseUnavailable, "database is unavailable"),
                nullptr);
        }
    });
}

void StorageImpl::asyncGetRows(const TableInfo::Ptr& _tableInfo,
    const std::shared_ptr<std::vector<std::string>>& _keys,
    std::function<void(const Error::Ptr&, const std::map<std::string, Entry::Ptr>&)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));
    m_threadPool->enqueue([_tableInfo, _keys, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            auto ret = storage->getRows(_tableInfo, *_keys);
            _callback(nullptr, ret);
        }
        else
        {
            _callback(make_shared<Error>(
                          StorageErrorCode::DataBaseUnavailable, "database is unavailable"),
                std::map<std::string, Entry::Ptr>());
        }
    });
}

void StorageImpl::asyncCommitBlock(protocol::BlockNumber _blockNumber,
    const std::shared_ptr<std::vector<std::shared_ptr<TableInfo>>>& _infos,
    const std::shared_ptr<std::vector<std::shared_ptr<std::map<std::string, Entry::Ptr>>>>& _datas,
    std::function<void(const Error::Ptr&, size_t)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));

    m_threadPool->enqueue([_infos, _datas, _blockNumber, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            auto ret = storage->commitBlock(_blockNumber, *_infos, *_datas);
            _callback(ret.second, ret.first);
        }
        else
        {
            _callback(make_shared<Error>(
                          StorageErrorCode::DataBaseUnavailable, "database is unavailable"),
                0);
        }
    });
}

void StorageImpl::asyncAddStateCache(protocol::BlockNumber _blockNumber,
    const std::shared_ptr<TableFactoryInterface>& _tablefactory,
    std::function<void(const Error::Ptr&)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));

    m_threadPool->enqueue([_blockNumber, _tablefactory, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            storage->addStateCache(_blockNumber, _tablefactory);
            _callback(nullptr);
        }
        else
        {
            _callback(make_shared<Error>(
                StorageErrorCode::DataBaseUnavailable, "database is unavailable"));
        }
    });
}

void StorageImpl::asyncDropStateCache(
    protocol::BlockNumber _blockNumber, std::function<void(const Error::Ptr&)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));

    m_threadPool->enqueue([_blockNumber, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            storage->dropStateCache(_blockNumber);
            _callback(nullptr);
        }
        else
        {
            _callback(make_shared<Error>(
                StorageErrorCode::DataBaseUnavailable, "database is unavailable"));
        }
    });
}

void StorageImpl::asyncGetStateCache(protocol::BlockNumber _blockNumber,
    std::function<void(const Error::Ptr&, const std::shared_ptr<TableFactoryInterface>&)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));

    m_threadPool->enqueue([_blockNumber, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            auto ret = storage->getStateCache(_blockNumber);
            _callback(nullptr, ret);
        }
        else
        {
            _callback(make_shared<Error>(
                          StorageErrorCode::DataBaseUnavailable, "database is unavailable"),
                nullptr);
        }
    });
}

std::shared_ptr<TableFactoryInterface> StorageImpl::getStateCache(
    protocol::BlockNumber _blockNumber)
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
    protocol::BlockNumber _blockNumber, const std::shared_ptr<TableFactoryInterface>& _tablefactory)
{
    std::unique_lock lock(m_number2TableFactoryMutex);
    m_number2TableFactory[_blockNumber] = _tablefactory;
}

Error::Ptr StorageImpl::put(const std::string_view& _columnFamily, const std::string_view& key,
    const std::string_view& value)
{
    return m_kvDB->put(_columnFamily, key, value);
}

std::pair<std::string, Error::Ptr> StorageImpl::get(
    const std::string_view& _columnFamily, const std::string_view& _key)
{
    return m_kvDB->get(_columnFamily, _key);
}

Error::Ptr StorageImpl::remove(const std::string_view& _columnFamily, const std::string_view& _key)
{
    return m_kvDB->remove(_columnFamily, _key);
}

void StorageImpl::asyncPut(const string_view& _columnFamily, const string_view& _key,
    const string_view& _value, std::function<void(const Error::Ptr&)> _callback)
{
    auto db = std::weak_ptr<KVDBInterface>(m_kvDB);
    m_threadPool->enqueue([columnFamily = string(_columnFamily), key = string(_key),
                              value = string(_value), _callback, db]() {
        auto kvDB = db.lock();
        if (kvDB)
        {
            auto ret = kvDB->put(columnFamily, key, value);
            if (ret)
            {
                _callback(ret);
            }
            else
            {
                _callback(nullptr);
            }
        }
        else
        {
            _callback(make_shared<Error>(
                StorageErrorCode::DataBaseUnavailable, "database is unavailable"));
        }
    });
}

void StorageImpl::asyncGet(const string_view& _columnFamily, const string_view& _key,
    std::function<void(const Error::Ptr&, const std::string& value)> _callback)
{
    auto db = std::weak_ptr<KVDBInterface>(m_kvDB);
    m_threadPool->enqueue(
        [columnFamily = string(_columnFamily), key = string(_key), _callback, db]() {
            auto kvDB = db.lock();
            if (kvDB)
            {
                auto ret = kvDB->get(columnFamily, key);
                _callback(ret.second, ret.first);
            }
            else
            {
                _callback(make_shared<Error>(
                              StorageErrorCode::DataBaseUnavailable, "database is unavailable"),
                    "");
            }
        });
}

void StorageImpl::asyncRemove(const string_view& _columnFamily, const string_view& _key,
    std::function<void(const Error::Ptr&)> _callback)
{
    auto db = std::weak_ptr<KVDBInterface>(m_kvDB);
    m_threadPool->enqueue(
        [columnFamily = string(_columnFamily), key = string(_key), _callback, db]() {
            auto kvDB = db.lock();
            if (kvDB)
            {
                auto ret = kvDB->remove(columnFamily, key);
                if (ret)
                {
                    _callback(nullptr);
                }
                else
                {
                }
            }
            else
            {
                _callback(make_shared<Error>(
                    StorageErrorCode::DataBaseUnavailable, "database is unavailable"));
            }
        });
}

void StorageImpl::asyncGetBatch(const string_view& _columnFamily,
    const std::shared_ptr<std::vector<std::string>>& _keys,
    std::function<void(const Error::Ptr&, const std::shared_ptr<std::vector<std::string>>&)>
        _callback)
{
    auto db = std::weak_ptr<KVDBInterface>(m_kvDB);
    m_threadPool->enqueue([columnFamily = string(_columnFamily), _keys, _callback, db]() {
        auto kvDB = db.lock();
        if (kvDB)
        {
            auto values = kvDB->multiGet(columnFamily, *_keys);
            _callback(nullptr, values);
        }
        else
        {
            _callback(make_shared<Error>(
                          StorageErrorCode::DataBaseUnavailable, "database is unavailable"),
                nullptr);
        }
    });
}

}  // namespace storage
}  // namespace bcos
