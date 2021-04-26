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
StorageImpl::StorageImpl(std::shared_ptr<RocksDBAdapterFactory> _stateDBFactory, size_t _poolSize)
  : m_stateDBFactory(_stateDBFactory)
{
    m_stateDB = m_stateDBFactory->createAdapter("/rocksDB", RocksDBAdapter::TABLE_PERFIX_LENGTH);
    m_db = shared_ptr<rocksdb::DB>(m_stateDBFactory->createRocksDB("/local"));
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
size_t StorageImpl::commitTables(const std::vector<std::shared_ptr<TableInfo>> _infos,
    std::vector<std::shared_ptr<std::map<std::string, std::shared_ptr<Entry>>>>& _datas)
{
    return m_stateDB->commitTables(_infos, _datas);
}

void StorageImpl::asyncGetPrimaryKeys(std::shared_ptr<TableInfo> _tableInfo,
    std::shared_ptr<Condition> _condition,
    std::function<void(Error, std::vector<std::string>)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));
    m_threadPool->enqueue([_tableInfo, _condition, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            auto ret = storage->getPrimaryKeys(_tableInfo, _condition);
            _callback(Error(), ret);
        }
        else
        {
            _callback(
                Error(StorageInterface::ErrorCode::DataBase_Unavailable, "database is unavailable"),
                std::vector<std::string>());
        }
    });
}

void StorageImpl::asyncGetRow(std::shared_ptr<TableInfo> _tableInfo,
    std::shared_ptr<std::string> _key, std::function<void(Error, std::shared_ptr<Entry>)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));
    m_threadPool->enqueue([_tableInfo, _key, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            auto ret = storage->getRow(_tableInfo, *_key);
            _callback(Error(), ret);
        }
        else
        {
            _callback(
                Error(StorageInterface::ErrorCode::DataBase_Unavailable, "database is unavailable"),
                nullptr);
        }
    });
}

void StorageImpl::asyncGetRows(std::shared_ptr<TableInfo> _tableInfo,
    std::shared_ptr<std::vector<std::string>> _keys,
    std::function<void(Error, std::map<std::string, std::shared_ptr<Entry>>)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));
    m_threadPool->enqueue([_tableInfo, _keys, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            auto ret = storage->getRows(_tableInfo, *_keys);
            _callback(Error(), ret);
        }
        else
        {
            _callback(
                Error(StorageInterface::ErrorCode::DataBase_Unavailable, "database is unavailable"),
                std::map<std::string, std::shared_ptr<Entry>>());
        }
    });
}

void StorageImpl::asyncCommitTables(std::shared_ptr<std::vector<std::shared_ptr<TableInfo>>> _infos,
    std::shared_ptr<std::vector<std::shared_ptr<std::map<std::string, Entry::Ptr>>>>& _datas,
    std::function<void(Error, size_t)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));

    m_threadPool->enqueue([_infos, _datas, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            auto ret = storage->commitTables(*_infos, *_datas);
            _callback(Error(), ret);
        }
        else
        {
            _callback(
                Error(StorageInterface::ErrorCode::DataBase_Unavailable, "database is unavailable"),
                0);
        }
    });
}

void StorageImpl::asyncAddStateCache(protocol::BlockNumber _blockNumber,
    protocol::Block::Ptr _block, std::shared_ptr<TableFactory> _tablefactory,
    std::function<void(Error)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));

    m_threadPool->enqueue([_blockNumber, _block, _tablefactory, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            storage->addStateCache(_blockNumber, _block, _tablefactory);
            _callback(Error());
        }
        else
        {
            _callback(Error(
                StorageInterface::ErrorCode::DataBase_Unavailable, "database is unavailable"));
        }
    });
}

void StorageImpl::asyncDropStateCache(
    protocol::BlockNumber _blockNumber, std::function<void(Error)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));

    m_threadPool->enqueue([_blockNumber, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            storage->dropStateCache(_blockNumber);
            _callback(Error());
        }
        else
        {
            _callback(Error(
                StorageInterface::ErrorCode::DataBase_Unavailable, "database is unavailable"));
        }
    });
}

void StorageImpl::asyncGetBlock(
    protocol::BlockNumber _blockNumber, std::function<void(Error, protocol::Block::Ptr)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));

    m_threadPool->enqueue([_blockNumber, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            auto ret = storage->getBlock(_blockNumber);
            _callback(Error(), ret);
        }
        else
        {
            _callback(
                Error(StorageInterface::ErrorCode::DataBase_Unavailable, "database is unavailable"),
                nullptr);
        }
    });
}

void StorageImpl::asyncGetStateCache(protocol::BlockNumber _blockNumber,
    std::function<void(Error, std::shared_ptr<TableFactory>)> _callback)
{
    auto self =
        std::weak_ptr<StorageImpl>(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));

    m_threadPool->enqueue([_blockNumber, _callback, self]() {
        auto storage = self.lock();
        if (storage)
        {
            auto ret = storage->getStateCache(_blockNumber);
            _callback(Error(), ret);
        }
        else
        {
            _callback(
                Error(StorageInterface::ErrorCode::DataBase_Unavailable, "database is unavailable"),
                nullptr);
        }
    });
}

protocol::Block::Ptr StorageImpl::getBlock(protocol::BlockNumber _blockNumber)
{
    std::shared_lock lock(m_number2TableFactoryMutex);
    if (m_number2TableFactory.count(_blockNumber))
    {
        return m_number2TableFactory[_blockNumber].block;
    }
    return nullptr;
}
std::shared_ptr<TableFactory> StorageImpl::getStateCache(protocol::BlockNumber _blockNumber)
{
    std::shared_lock lock(m_number2TableFactoryMutex);
    if (m_number2TableFactory.count(_blockNumber))
    {
        return m_number2TableFactory[_blockNumber].tableFactory;
    }
    return nullptr;
}
void StorageImpl::dropStateCache(protocol::BlockNumber _blockNumber)
{
    std::unique_lock lock(m_number2TableFactoryMutex);
    m_number2TableFactory.erase(_blockNumber);
}

void StorageImpl::addStateCache(protocol::BlockNumber _blockNumber, protocol::Block::Ptr _block,
    std::shared_ptr<TableFactory> _tablefactory)
{
    std::unique_lock lock(m_number2TableFactoryMutex);
    m_number2TableFactory[_blockNumber] = BlockCache{_block, _tablefactory};
}

bool StorageImpl::put(
    const std::string& _columnFamily, const std::string_view& key, const std::string_view& value)
{
    rocksdb::ColumnFamilyHandle* cf = nullptr;
    auto s = m_db->CreateColumnFamily(ColumnFamilyOptions(), _columnFamily, &cf);
    assert(s.ok());
    m_db->Put(WriteOptions(), cf, Slice(key.data(), key.size()), Slice(value.data(), value.size()));
    s = m_db->DestroyColumnFamilyHandle(cf);
    return s.ok();
}

std::string StorageImpl::get(const std::string& _columnFamily, const std::string_view& key)
{
    rocksdb::ColumnFamilyHandle* cf = nullptr;
    auto s = m_db->CreateColumnFamily(ColumnFamilyOptions(), _columnFamily, &cf);
    assert(s.ok());
    string value;
    m_db->Get(ReadOptions(), cf, Slice(key.data(), key.size()), &value);
    s = m_db->DestroyColumnFamilyHandle(cf);
    assert(s.ok());
    return value;
}

void StorageImpl::asyncGetBatch(const std::string& _columnFamily,
    std::shared_ptr<std::vector<std::string_view>> _keys,
    std::function<void(Error, std::shared_ptr<std::vector<std::string>>)> _callback)
{
    auto db = std::weak_ptr<rocksdb::DB>(std::dynamic_pointer_cast<rocksdb::DB>(m_db));
    m_threadPool->enqueue([_columnFamily, _keys, _callback, db]() {
        auto rocksdb = db.lock();
        if (rocksdb)
        {
            rocksdb::ColumnFamilyHandle* cf = nullptr;
            auto s = rocksdb->CreateColumnFamily(ColumnFamilyOptions(), _columnFamily, &cf);
            assert(s.ok());
            vector<Slice> keys;
            keys.reserve(_keys->size());
            for (auto& key : *_keys)
            {
                keys.emplace_back(key.data(), key.size());
            }
            auto values = make_shared<vector<string>>();
            rocksdb->MultiGet(ReadOptions(), std::vector<ColumnFamilyHandle*>(keys.size(), cf),
                keys, values.get());
            s = rocksdb->DestroyColumnFamilyHandle(cf);
            assert(s.ok());
            _callback(Error(), values);
        }
        else
        {
            _callback(
                Error(StorageInterface::ErrorCode::DataBase_Unavailable, "database is unavailable"),
                nullptr);
        }
    });
}

}  // namespace storage
}  // namespace bcos
