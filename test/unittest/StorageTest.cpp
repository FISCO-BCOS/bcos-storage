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
 * @brief the test of Storage
 * @file StorageTest.cpp
 */

#include "Storage.h"
#include "KVDBImpl.h"
#include "MemoryDB.h"
#include "RocksDBAdapter/RocksDBAdapter.h"
#include "RocksDBAdapter/RocksDBAdapterFactory.h"
#include "bcos-framework/libprotocol/protobuf/PBBlock.h"
#include "bcos-framework/libprotocol/protobuf/PBBlockFactory.h"
#include "bcos-framework/libprotocol/protobuf/PBBlockHeaderFactory.h"
#include "bcos-framework/libprotocol/protobuf/PBTransactionFactory.h"
#include "bcos-framework/libprotocol/protobuf/PBTransactionReceiptFactory.h"
#include "bcos-framework/libtable/TableFactory.h"
#include "bcos-framework/testutils/HashImpl.h"
#include "bcos-framework/testutils/SignatureImpl.h"
#include "boost/filesystem.hpp"
#include "rocksdb/db.h"
#include <boost/test/unit_test.hpp>

using namespace std;
using namespace bcos;
using namespace bcos::storage;
using namespace bcos::protocol;
using namespace bcos::crypto;
namespace fs = boost::filesystem;

namespace bcos
{
namespace test
{
struct StorageFixture
{
    StorageFixture()
    {
        factory = make_shared<RocksDBAdapterFactory>(testPath.string());
        memoryStorage = make_shared<MemoryStorage>();
        testTableInfo =
            std::make_shared<storage::TableInfo>(testTableName, testTableKey, "value1,value2");
        testTableInfo->newTable = true;
        auto ret = factory->createRocksDB("test_db_2", RocksDBAdapter::TABLE_PERFIX_LENGTH);
        kvDB = make_shared<KVDBImpl>(ret.first);
        storage = make_shared<StorageImpl>(memoryStorage, kvDB);
    }
    ~StorageFixture()
    {
        if (fs::exists(testPath))
        {
            fs::remove_all(testPath);
        }
    }
    fs::path testPath = "./unittest_db";
    std::shared_ptr<AdapterInterface> memoryStorage = nullptr;
    std::shared_ptr<RocksDBAdapterFactory> factory = nullptr;
    std::shared_ptr<KVDBImpl> kvDB = nullptr;
    std::shared_ptr<StorageImpl> storage = nullptr;
    std::shared_ptr<TableInfo> testTableInfo = nullptr;
    string testTableName = "t_test";
    string testTableKey = "key";
};
BOOST_FIXTURE_TEST_SUITE(StorageTest, StorageFixture)

BOOST_AUTO_TEST_CASE(commitTables)
{
    // if this ut failed, please `rm -rf unittest_db` and try again
    auto infos = vector<TableInfo::Ptr>();
    auto datas = vector<shared_ptr<map<string, Entry::Ptr>>>();
    infos.push_back(testTableInfo);
    auto ret = storage->commitTables(infos, datas);
    BOOST_TEST(ret == 0);

    auto tableData = make_shared<map<string, Entry::Ptr>>();

    size_t count = 100;
    for (size_t i = 0; i < count; ++i)
    {
        auto entry = make_shared<Entry>();
        entry->setField(testTableKey, to_string(i));
        entry->setField("value1", to_string(i + 1));
        entry->setField("value2", to_string(i + 2));
        (*tableData)[to_string(i)] = entry;
    }
    auto entry = make_shared<Entry>();
    entry->setField(testTableKey, to_string(count + 1));
    entry->setField("value1", to_string(count + 1));
    entry->setField("value2", to_string(count + 2));
    entry->setStatus(Entry::Status::DELETED);
    (*tableData)[to_string(count + 1)] = entry;

    datas.push_back(tableData);
    ret = storage->commitTables(infos, datas);
    BOOST_TEST(ret == count);

    for (size_t i = 0; i < count; ++i)
    {
        auto entry = storage->getRow(testTableInfo, to_string(i));
        BOOST_TEST(entry != nullptr);
        BOOST_TEST(entry->getField("value1") == to_string(i + 1));
        BOOST_TEST(entry->getField("value2") == to_string(i + 2));
        BOOST_TEST(entry->getField(testTableKey) == to_string(i));
        BOOST_TEST(entry->num() == 0);
        // BOOST_TEST(entry->dirty() == false);
        BOOST_TEST(entry->getStatus() == Entry::Status::NORMAL);
    }

    auto keys = storage->getPrimaryKeys(testTableInfo, nullptr);
    BOOST_TEST(keys.size() == count);
    auto entries = storage->getRows(testTableInfo, keys);
    BOOST_TEST(entries.size() == count);
    for (size_t i = 0; i < count; ++i)
    {
        auto entry = entries[to_string(i)];
        BOOST_TEST(entry != nullptr);
        BOOST_TEST(entry->getField(testTableKey) == to_string(i));
        BOOST_TEST(entry->getField("value1") == to_string(i + 1));
        BOOST_TEST(entry->getField("value2") == to_string(i + 2));
        BOOST_TEST(entry->num() == 0);
        // BOOST_TEST(entry->dirty() == false);
        BOOST_TEST(entry->getStatus() == Entry::Status::NORMAL);
    }
}

BOOST_AUTO_TEST_CASE(asyncInterfaces)
{
    auto infos = vector<TableInfo::Ptr>();
    auto datas = vector<shared_ptr<map<string, Entry::Ptr>>>();
    infos.push_back(testTableInfo);
    auto ret = storage->commitTables(infos, datas);
    BOOST_TEST(ret == 0);

    auto tableData = make_shared<map<string, Entry::Ptr>>();

    size_t count = 100;
    for (size_t i = 0; i < count; ++i)
    {
        auto entry = make_shared<Entry>();
        entry->setField(testTableKey, to_string(i));
        entry->setField("value1", to_string(i + 1));
        entry->setField("value2", to_string(i + 2));
        (*tableData)[to_string(i)] = entry;
    }
    auto entry = make_shared<Entry>();
    entry->setField(testTableKey, to_string(count + 1));
    entry->setField("value1", to_string(count + 1));
    entry->setField("value2", to_string(count + 2));
    entry->setStatus(Entry::Status::DELETED);
    (*tableData)[to_string(count + 1)] = entry;

    datas.push_back(tableData);
    ret = storage->commitTables(infos, datas);
    BOOST_TEST(ret == count);
    // add ut for asyncGetPrimaryKeys
    struct Callback : public std::enable_shared_from_this<Callback>
    {
    public:
        typedef std::shared_ptr<Callback> Ptr;

        explicit Callback(size_t _value, size_t _total) : value(_value), total(_total)
        {
            mutex.lock();
        }

        void onResult(Error _error, std::vector<std::string> _result)
        {
            BOOST_TEST(_error.errorCode() == 0);
            // include [6, 7, 8, 9]
            BOOST_TEST(_result.size() == total);
            auto valueStr = to_string(value);
            for (auto& v : _result)
            {
                if (v >= valueStr)
                {
                    BOOST_TEST(true);
                    continue;
                }
                BOOST_TEST(false);
            }
            mutex.unlock();
        }
        size_t value = 0;
        size_t total;
        std::mutex mutex;
    };
    size_t min = 50;
    Callback::Ptr callback = std::make_shared<Callback>(min, 54);
    std::function<void(Error, std::vector<std::string>)> fp =
        std::bind(&Callback::onResult, callback, std::placeholders::_1, std::placeholders::_2);
    auto condition = make_shared<Condition>();
    condition->GE(to_string(min));
    storage->asyncGetPrimaryKeys(testTableInfo, condition, fp);
    // lock to wait for async send
    callback->mutex.lock();
    callback->mutex.unlock();
    // nullptr condition
    min = 0;
    callback = std::make_shared<Callback>(min, 100);
    fp = std::bind(&Callback::onResult, callback, std::placeholders::_1, std::placeholders::_2);
    storage->asyncGetPrimaryKeys(testTableInfo, nullptr, fp);
    // lock to wait for async send
    callback->mutex.lock();
    callback->mutex.unlock();

    // add ut for asyncGetRows
    struct Callback2 : public std::enable_shared_from_this<Callback2>
    {
        typedef std::shared_ptr<Callback2> Ptr;
        explicit Callback2(shared_ptr<vector<string>> _keys) : keys(_keys) { mutex.lock(); }

        void onResult(Error _error, std::map<std::string, std::shared_ptr<Entry>> _result)
        {
            BOOST_TEST(_error.errorCode() == 0);
            BOOST_TEST(_result.size() == keys->size());
            for (size_t i = 0; i < keys->size(); ++i)
            {
                auto entry = _result[keys->at(i)];
                BOOST_TEST(entry != nullptr);
                // BOOST_TEST(entry->getField(testTableKey) == to_string(i));
                BOOST_TEST(entry->getField("value1") == to_string(i + 1));
                BOOST_TEST(entry->getField("value2") == to_string(i + 2));
                BOOST_TEST(entry->num() == 0);
                // BOOST_TEST(entry->dirty() == false);
                BOOST_TEST(entry->getStatus() == Entry::Status::NORMAL);
            }
            mutex.unlock();
        }
        shared_ptr<vector<string>> keys = nullptr;
        std::mutex mutex;
    };
    auto keys = make_shared<vector<string>>();
    for (size_t i = 0; i < count; ++i)
    {
        keys->emplace_back(to_string(i));
    }

    Callback2::Ptr callback2 = std::make_shared<Callback2>(keys);
    std::function<void(Error, std::map<std::string, std::shared_ptr<Entry>>)> fp2 =
        std::bind(&Callback2::onResult, callback2, std::placeholders::_1, std::placeholders::_2);
    storage->asyncGetRows(testTableInfo, keys, fp2);
    // lock to wait for async send
    callback2->mutex.lock();
    callback2->mutex.unlock();

    // add ut for asyncGetRow
    struct Callback3 : public std::enable_shared_from_this<Callback3>
    {
        typedef std::shared_ptr<Callback3> Ptr;
        explicit Callback3(size_t _key) : key(_key) { mutex.lock(); }

        void onResult(Error _error, std::shared_ptr<Entry> _result)
        {
            BOOST_TEST(_error.errorCode() == 0);
            BOOST_TEST(_result != nullptr);
            // BOOST_TEST(_result->getField(testTableKey) == to_string(i));
            BOOST_TEST(_result->getField("value1") == to_string(key + 1));
            BOOST_TEST(_result->getField("value2") == to_string(key + 2));
            BOOST_TEST(_result->num() == 0);
            // BOOST_TEST(_result->dirty() == false);
            BOOST_TEST(_result->getStatus() == Entry::Status::NORMAL);
            mutex.unlock();
        }
        size_t key = 0;
        std::mutex mutex;
    };
    auto key = make_shared<string>("56");
    Callback3::Ptr callback3 = std::make_shared<Callback3>(56);
    std::function<void(Error, std::shared_ptr<Entry>)> fp3 =
        std::bind(&Callback3::onResult, callback3, std::placeholders::_1, std::placeholders::_2);
    storage->asyncGetRow(testTableInfo, key, fp3);
    // lock to wait for async send
    callback3->mutex.lock();
    callback3->mutex.unlock();

    // TODO: add ut for asyncCommitTables
}


BOOST_AUTO_TEST_CASE(TableFactory_cache)
{
    auto infos = vector<TableInfo::Ptr>();
    auto datas = vector<shared_ptr<map<string, Entry::Ptr>>>();
    infos.push_back(testTableInfo);
    auto ret = storage->commitTables(infos, datas);
    BOOST_TEST(ret == 0);

    auto tableData = make_shared<map<string, Entry::Ptr>>();

    size_t count = 100;
    for (size_t i = 0; i < count; ++i)
    {
        auto entry = make_shared<Entry>();
        entry->setField(testTableKey, to_string(i));
        entry->setField("value1", to_string(i + 1));
        entry->setField("value2", to_string(i + 2));
        (*tableData)[to_string(i)] = entry;
    }
    auto entry = make_shared<Entry>();
    entry->setField(testTableKey, to_string(count + 1));
    entry->setField("value1", to_string(count + 1));
    entry->setField("value2", to_string(count + 2));
    entry->setStatus(Entry::Status::DELETED);
    (*tableData)[to_string(count + 1)] = entry;

    datas.push_back(tableData);
    ret = storage->commitTables(infos, datas);
    BOOST_TEST(ret == count);
    // add ut for addStateCache
    vector<protocol::PBBlock::Ptr> blocks;
    vector<TableFactory::Ptr> tfs;
    auto hashImpl = std::make_shared<Sm3Hash>();
    auto signImpl = std::make_shared<SM2SignatureImpl>();
    auto cryptoSuite =  std::make_shared<CryptoSuite>(hashImpl, signImpl, nullptr);
    auto blockHeaderFactory = std::make_shared<PBBlockHeaderFactory>(cryptoSuite);
    auto transactionFactory = std::make_shared<PBTransactionFactory>(cryptoSuite);
    auto receiptFactory = std::make_shared<PBTransactionReceiptFactory>(cryptoSuite);

    for (size_t i = 0; i < count; ++i)
    {
        auto block = make_shared<protocol::PBBlock>(blockHeaderFactory, transactionFactory, receiptFactory);
        blocks.push_back(block);
        auto tableFactory = make_shared<TableFactory>(storage, hashImpl, i);
        tfs.push_back(tableFactory);
        storage->addStateCache(i, block, tableFactory);
    }
    for (size_t i = 0; i < count; ++i)
    {  // ut for getBlock
        auto block = storage->getBlock(i);
        BOOST_TEST(block == blocks[i]);
        // add ut for getStateCache
        auto tf = storage->getStateCache(i);
        BOOST_TEST(tf == tfs[i]);
    }
    auto block = storage->getBlock(count + 1);
    BOOST_TEST(block == nullptr);
    // add ut for getStateCache
    auto tf = storage->getStateCache(count + 1);
    BOOST_TEST(tf == nullptr);

    // ut for dropStateCache
    storage->dropStateCache(count - 1);
    block = storage->getBlock(count - 1);
    BOOST_TEST(block == nullptr);
    // add ut for getStateCache
    tf = storage->getStateCache(count - 1);
    BOOST_TEST(tf == nullptr);

    // TODO: add ut for asyncAddStateCache
    // TODO: add ut for asyncDropStateCache
    // TODO: add ut for asyncGetBlock
    // TODO: add ut for asyncGetStateCache
}

BOOST_AUTO_TEST_CASE(KVInterfaces)
{
    // if this ut failed, please `rm -rf unittest_db` and try again
    size_t count = 10;
    for (size_t i = 0; i < count; ++i)
    {
        auto column = to_string(i);
        auto key = to_string(i + 1);
        auto value = to_string(i + 2);
        auto ret = storage->put(column, key, value);
        BOOST_TEST(ret == true);
    }
    for (size_t i = 0; i < count; ++i)
    {
        auto column = to_string(i);
        auto key = to_string(i + 1);
        auto value = to_string(i + 2);
        auto retValue = storage->get(column, key);
        BOOST_TEST(retValue == value);
    }

    struct Callback : public std::enable_shared_from_this<Callback>
    {
    public:
        typedef std::shared_ptr<Callback> Ptr;

        explicit Callback(size_t _value) : value(_value) { mutex.lock(); }

        void onResult(Error _error, std::shared_ptr<std::vector<std::string>> _result)
        {
            BOOST_TEST(_error.errorCode() == 0);
            BOOST_TEST(_result->size() == 1);
            auto originValue = to_string(value);
            auto retValue = (*_result)[0];
            BOOST_TEST(retValue == originValue);
            mutex.unlock();
        }
        size_t value = 0;
        std::mutex mutex;
    };

    for (size_t i = 0; i < count; ++i)
    {
        auto column = to_string(i);
        auto key = to_string(i + 1);
        std::shared_ptr<std::vector<std::string_view>> pkeys = make_shared<vector<string_view>>();
        pkeys->push_back(key);
        Callback::Ptr callback = std::make_shared<Callback>(i + 2);
        std::function<void(Error, std::shared_ptr<std::vector<std::string>>)> fp =
            std::bind(&Callback::onResult, callback, std::placeholders::_1, std::placeholders::_2);
        storage->asyncGetBatch(column, pkeys, fp);
        // lock to wait for async send
        callback->mutex.lock();
        callback->mutex.unlock();
    }
}

BOOST_AUTO_TEST_SUITE_END()
}  // namespace test
}  // namespace bcos
