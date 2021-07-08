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
#include "bcos-storage/Storage.h"
#include "bcos-framework/interfaces/storage/StorageInterface.h"
#include "bcos-framework/libtable/TableFactory.h"
#include "bcos-framework/libutilities/ThreadPool.h"
#include "rocksdb/db.h"
#include "tbb/parallel_for_each.h"

using namespace std;
using namespace rocksdb;

namespace bcos
{
namespace storage
{
StorageImpl::StorageImpl(std::shared_ptr<AdapterInterface> _stateDB,
    std::shared_ptr<KVDBInterface> _kvDB, size_t _poolSize, int32_t _clearInterval,
    int64_t _maxCapacity)
  : m_stateDB(_stateDB), m_kvDB(_kvDB), m_maxCapacity(_maxCapacity), m_clearInterval(_clearInterval)
{
    assert(m_stateDB);
    assert(m_kvDB);
    m_threadPool = std::make_shared<bcos::ThreadPool>("asyncTasks", _poolSize);
    m_asyncThread = std::make_shared<bcos::ThreadPool>("mru", 1);

    m_running = std::make_shared<tbb::atomic<bool>>(true);
}

void StorageImpl::start()
{
    m_running->store(true);
    std::weak_ptr<StorageImpl> self(std::dynamic_pointer_cast<StorageImpl>(shared_from_this()));
    auto running = m_running;
    if (m_enableCache)
    {
        m_mruQueue = std::make_shared<
            tbb::concurrent_queue<std::tuple<std::string, std::string, ssize_t>>>();
        m_mru = std::make_shared<boost::multi_index_container<std::pair<std::string, std::string>,
            boost::multi_index::indexed_by<boost::multi_index::sequenced<>,
                boost::multi_index::hashed_unique<
                    boost::multi_index::identity<std::pair<std::string, std::string>>>>>>();
        m_clearThread = std::make_shared<std::thread>([running, self]() {
            pthread_setThreadName("MemClear");
            while (true)
            {
                auto storage = self.lock();
                if (storage && running->load())
                {
                    std::this_thread::sleep_for(std::chrono::seconds(storage->m_clearInterval));
                    storage->checkAndClear();
                }
                else
                {
                    return;
                }
            }
        });
    }
}


void StorageImpl::stop()
{
    if (!m_running)
    {
        STORAGE_LOG(WARNING) << LOG_DESC("The storage has already been stopped");
        return;
    }
    STORAGE_LOG(INFO) << LOG_DESC("Stop the storage");
    m_running->store(false);
    if (m_threadPool)
    {
        m_threadPool->stop();
    }
    if (m_asyncThread)
    {
        m_asyncThread->stop();
    }
    if (m_clearThread)
    {
        m_clearThread->join();
    }
    STORAGE_LOG(INFO) << LOG_DESC("Stop the storage success");
}

std::vector<std::string> StorageImpl::getPrimaryKeys(
    const TableInfo::Ptr& _tableInfo, const Condition::Ptr& _condition) const
{
    return m_stateDB->getPrimaryKeys(_tableInfo, _condition);
}

Entry::Ptr StorageImpl::getRow(const TableInfo::Ptr& _tableInfo, const std::string_view& _key)
{
    if (m_enableCache)
    {
        auto result = touchCache(_tableInfo->name, _key);
        auto cache = std::get<1>(result);
        auto entry = cache->entry();
        if (cache->empty())
        {
            entry = m_stateDB->getRow(_tableInfo, _key);
            if (entry)
            {
                touchMRU(_tableInfo->name, _key, entry->capacityOfHashField());
                cache->setEntry(entry);
                cache->setEmpty(false);
            }
        }

        return entry;
    }
    return m_stateDB->getRow(_tableInfo, _key);
}

std::map<std::string, Entry::Ptr> StorageImpl::getRows(
    const TableInfo::Ptr& _tableInfo, const std::vector<std::string>& _keys)
{
    if (m_enableCache)
    {  // find m_caches first
        std::map<std::string, Entry::Ptr> ret;
        std::mutex retMutex;
        tbb::parallel_for(tbb::blocked_range<size_t>(0, _keys.size()),
            [&](const tbb::blocked_range<size_t>& range) {
                for (size_t i = range.begin(); i < range.end(); ++i)
                {
                    auto entry = getRow(_tableInfo, _keys[i]);
                    {
                        lock_guard l(retMutex);
                        ret[_keys[i]] = entry;
                    }
                }
            });
        return ret;
    }
    return m_stateDB->getRows(_tableInfo, _keys);
}

std::pair<size_t, Error::Ptr> StorageImpl::commitBlock(protocol::BlockNumber _number,
    const std::vector<TableInfo::Ptr>& _infos,
    const std::vector<std::shared_ptr<std::map<std::string, Entry::Ptr>>>& _datas)
{
    // merge state cache then commit
    std::shared_ptr<TableFactoryInterface> stateTableFactory = nullptr;
    {
        std::shared_lock lock(m_number2TableFactoryMutex);
        if (m_number2TableFactory.count(_number))
        {
            stateTableFactory = m_number2TableFactory.at(_number);
        }
    }
    std::pair<size_t, Error::Ptr> ret;
    if (stateTableFactory)
    {
        STORAGE_LOG(INFO) << LOG_BADGE("StorageImpl") << LOG_DESC("commitBlock and state")
                          << LOG_KV("block", _number);
        auto stateData = stateTableFactory->exportData(_number);
        stateData.first.insert(stateData.first.end(), _infos.begin(), _infos.end());
        stateData.second.insert(stateData.second.end(), _datas.begin(), _datas.end());

        ret = m_stateDB->commitTables(stateData.first, stateData.second);
        if (!ret.second)
        {  // drop state cache, when commite succeed
            addStateToCache(std::move(stateData));
            dropStateCache(_number);
        }
    }
    else
    {
        // consensus will commit empty block without state
        ret = m_stateDB->commitTables(_infos, _datas);
        if (_number >= 0)
        {
            STORAGE_LOG(INFO) << LOG_BADGE("StorageImpl") << LOG_DESC("commitBlock")
                              << LOG_KV("block", _number);
        }
    }
    if (_number >= 0 && !ret.second)
    {
        m_blockNumber.store(_number);
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
            // m_callbackThreadPool->enqueue(
            //     [move(ret), move(_callback)]() { _callback(nullptr, ret); });
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
    // use ordered queue to commit block
    m_asyncThread->enqueue([_infos, _datas, _blockNumber, _callback, self]() {
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

void StorageImpl::addStateToCache(std::pair<std::vector<TableInfo::Ptr>,
    std::vector<std::shared_ptr<std::map<std::string, Entry::Ptr>>>>&& _data)
{
    if (m_enableCache)
    {  // add committed data to cache
        auto& tableInfos = _data.first;
        auto& tableDataVector = _data.second;
        tbb::parallel_for(tbb::blocked_range<size_t>(0, tableInfos.size()),
            [&](const tbb::blocked_range<size_t>& range) {
                for (size_t i = range.begin(); i < range.end(); ++i)
                {
                    auto tableInfo = tableInfos[i];
                    auto tableData = tableDataVector[i];
                    tbb::parallel_for_each(tableData->begin(), tableData->end(),
                        [&](std::pair<const std::string, Entry::Ptr>& data) {
                            auto result = touchCache(tableInfo->name, data.first);
                            auto cache = std::get<1>(result);
                            cache->setEntry(data.second);
                            cache->setEmpty(false);
                            touchMRU(
                                tableInfo->name, data.first, data.second->capacityOfHashField());
                        });
                }
            });
    }
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


std::tuple<std::shared_ptr<RWScoped>, Cache::Ptr, bool> StorageImpl::touchCache(
    const std::string& _tableName, const std::string_view& key)
{
    bool hit = true;

    ++m_queryTimes;

    auto cache = std::make_shared<Cache>();
    auto cacheKey = _tableName + "_" + string(key);

    bool inserted = false;
    {
        RWScoped lockCache(m_cachesMutex, false);

        auto result = m_caches.insert(std::make_pair(cacheKey, cache));

        cache = result.first->second;
        inserted = result.second;
    }

    auto cacheLock = std::make_shared<RWScoped>(*(cache->mutex()), true);
    if (inserted)
    {
        hit = false;
    }
    else
    {
        RWScoped lockCache(m_cachesMutex, false);

        auto result = m_caches.insert(std::make_pair(cacheKey, cache));
        if (!result.second && cache != result.first->second)
        {
            cache = result.first->second;
            cacheLock.reset();
            cacheLock = std::make_shared<RWScoped>(*(cache->mutex()), true);
        }
    }

    if (hit)
    {
        ++m_hitTimes;
    }

    return std::make_tuple(cacheLock, cache, true);
}

void StorageImpl::touchMRU(const std::string& table, const std::string_view& key, ssize_t capacity)
{
    m_asyncThread->enqueue([this, table, key = string(key), capacity]() {
        m_mruQueue->push(std::make_tuple(table, key, capacity));
    });
}

void StorageImpl::removeCache(const std::string& table, const std::string& key)
{
    auto cacheKey = table + "_" + key;
    RWScoped lockCache;

    while (true)
    {
        if (lockCache.try_acquire(m_cachesMutex, true))
        {
            break;
        }
        std::this_thread::yield();
    }

    auto c = m_caches.unsafe_erase(cacheKey);

    if (c != 1)
    {
        STORAGE_LOG(FATAL) << "Can not remove cache: " << table << "-" << key;
        exit(1);
    }
}

void StorageImpl::updateMRU(const std::string& table, const std::string& key, ssize_t capacity)
{
    if (capacity != 0)
    {
        updateCapacity(capacity);
    }

    auto r = m_mru->push_back(std::make_pair(table, key));
    if (!r.second)
    {
        m_mru->relocate(m_mru->end(), r.first);
    }
}

void StorageImpl::updateCapacity(ssize_t capacity)
{
    m_capacity.fetch_and_add(capacity);
}

std::string StorageImpl::readableCapacity(size_t num)
{
    std::stringstream capacityNum;

    if (num > 1024 * 1024 * 1024)
    {
        capacityNum << std::setiosflags(std::ios::fixed) << std::setprecision(4)
                    << ((double)num / (1024 * 1024 * 1024)) << " GB";
    }
    else if (num > 1024 * 1024)
    {
        capacityNum << std::setiosflags(std::ios::fixed) << std::setprecision(4)
                    << ((double)num / (1024 * 1024)) << " MB";
    }
    else if (num > 1024)
    {
        capacityNum << std::setiosflags(std::ios::fixed) << std::setprecision(4)
                    << ((double)num / (1024)) << " KB";
    }
    else
    {
        capacityNum << num << " B";
    }
    return capacityNum.str();
}

void StorageImpl::checkAndClear()
{
    uint64_t count = 0;
    // calculate and calculate m_capacity with all elements of m_mruQueue
    // since inner loop will break once m_mruQueue is empty, here use while(true)
    while (true)
    {
        std::tuple<std::string, std::string, ssize_t> mru;
        auto result = m_mruQueue->try_pop(mru);
        if (!result)
        {
            break;
        }
        updateMRU(std::get<0>(mru), std::get<1>(mru), std::get<2>(mru));
        ++count;
    }

    STORAGE_LOG(DEBUG) << "CheckAndClear pop: " << count << " elements";

    TIME_RECORD("Check and clear");

    bool needClear = false;
    size_t clearTimes = 0;

    auto currentCapacity = m_capacity.load();

    size_t clearCount = 0;
    size_t clearThrough = 0;
    do
    {
        needClear = false;
        if (m_blockNumber > 0)
        {
            if (m_capacity > m_maxCapacity && !m_mru->empty())
            {
                needClear = true;
            }
        }

        if (needClear)
        {
            for (auto it = m_mru->begin(); it != m_mru->end() && m_running->load();)
            {
                if (m_capacity <= (int64_t)m_maxCapacity || m_mru->empty())
                {
                    break;
                }

                ++clearThrough;

                // The life cycle of the cache must be longer than the result, because the
                // deconstruction order of the tuple is related to the gcc version and the compiler.
                // It must be ensured that RWScoped is deconstructed first, and the cache is
                // deconstructed, otherwise the program will coredump
                Cache::Ptr cache;
                auto result = touchCache(it->first, it->second);
                cache = std::get<1>(result);
                if (m_blockNumber > 0 && (cache->num() <= m_blockNumber))
                {
                    int64_t totalCapacity = cache->entry()->capacityOfHashField();

                    ++clearCount;
                    updateCapacity(0 - totalCapacity);

                    cache->setEmpty(true);
                    removeCache(it->first, it->second);
                    it = m_mru->erase(it);
                }
                else
                {
                    break;
                }
            }
            ++clearTimes;
        }
    } while (needClear && m_running->load());

    if (clearThrough > 0)
    {
        STORAGE_LOG(INFO) << "Clear finished, total: " << clearCount << " entries, "
                          << "through: " << clearThrough << " entries, "
                          << readableCapacity(currentCapacity - m_capacity)
                          << ", Current total entries: " << m_caches.size()
                          << ", Current total mru entries: " << m_mru->size()
                          << ", total capacaity: " << readableCapacity(m_capacity);

        STORAGE_LOG(DEBUG)
            << "Cache Status: \n\n"
            << "\n---------------------------------------------------------------------\n"
            << "Total query: " << m_queryTimes << "\n"
            << "Total cache hit: " << m_hitTimes << "\n"
            << "Total cache miss: " << m_queryTimes - m_hitTimes << "\n"
            << "Total hit ratio: " << std::setiosflags(std::ios::fixed) << std::setprecision(4)
            << ((double)m_hitTimes / m_queryTimes) * 100 << "%"
            << "\n\n"
            << "Cache capacity: " << readableCapacity(m_capacity) << "\n"
            << "Cache size: " << m_mru->size()
            << "\n---------------------------------------------------------------------\n";
    }
}

}  // namespace storage
}  // namespace bcos
