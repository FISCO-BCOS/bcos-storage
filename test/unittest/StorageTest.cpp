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

#include "bcos-storage/Storage.h"
#include "KVDBImpl.h"
#include "MemoryDB.h"
#include "RocksDBAdapter.h"
#include "RocksDBAdapterFactory.h"
#include "bcos-framework/libprotocol/protobuf/PBBlock.h"
#include "bcos-framework/libprotocol/protobuf/PBBlockFactory.h"
#include "bcos-framework/libprotocol/protobuf/PBBlockHeaderFactory.h"
#include "bcos-framework/libprotocol/protobuf/PBTransactionFactory.h"
#include "bcos-framework/libprotocol/protobuf/PBTransactionReceiptFactory.h"
#include "bcos-framework/libtable/TableFactory.h"
#include "bcos-framework/testutils/crypto/HashImpl.h"
#include "bcos-framework/testutils/crypto/SignatureImpl.h"
#include "boost/filesystem.hpp"
#include "rocksdb/db.h"
#include <boost/test/unit_test.hpp>
#include <future>

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
        storage = make_shared<StorageImpl>(memoryStorage, kvDB, 2, 1, 1);
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
    storage->start();
    protocol::BlockNumber blockNumber = 0;
    // if this ut failed, please `rm -rf unittest_db` and try again
    auto infos = vector<TableInfo::Ptr>();
    auto datas = vector<shared_ptr<map<string, Entry::Ptr>>>();
    infos.push_back(testTableInfo);
    auto ret = storage->commitBlock(blockNumber++, infos, datas);
    BOOST_TEST(ret.first == 0);

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
    auto hashImpl = std::make_shared<Sm3Hash>();
    auto tableFactory = make_shared<TableFactory>(storage, hashImpl, blockNumber);
    storage->addStateCache(blockNumber, tableFactory);
    ret = storage->commitBlock(blockNumber++, infos, datas);
    BOOST_TEST(ret.first == count);

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
    storage->stop();
}

BOOST_AUTO_TEST_CASE(asyncInterfaces)
{
    storage->start();

    protocol::BlockNumber blockNumber = 0;
    auto infos = vector<TableInfo::Ptr>();
    auto datas = vector<shared_ptr<map<string, Entry::Ptr>>>();
    infos.push_back(testTableInfo);
    auto ret = storage->commitBlock(blockNumber++, infos, datas);
    BOOST_TEST(ret.first == 0);

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
    auto hashImpl = std::make_shared<Sm3Hash>();
    auto tableFactory = make_shared<TableFactory>(storage, hashImpl, blockNumber);
    storage->addStateCache(blockNumber, tableFactory);
    ret = storage->commitBlock(blockNumber++, infos, datas);
    BOOST_TEST(ret.first == count);
    // add ut for asyncGetPrimaryKeys
    size_t min = 50;
    auto condition = make_shared<Condition>();
    condition->GE(to_string(min));
    std::promise<void> asyncGetPrimaryKeysProm1;

    storage->asyncGetPrimaryKeys(testTableInfo, condition,
        [&asyncGetPrimaryKeysProm1](
            const Error::Ptr& _error, const std::vector<std::string>& _result) {
            BOOST_TEST(_error == nullptr);
            // include [6, 7, 8, 9]
            BOOST_TEST(_result.size() == 54);
            auto valueStr = to_string(50);
            for (auto& v : _result)
            {
                if (v >= valueStr)
                {
                    BOOST_TEST(true);
                    continue;
                }
                BOOST_TEST(false);
            }
            asyncGetPrimaryKeysProm1.set_value();
        });
    asyncGetPrimaryKeysProm1.get_future().get();

    // nullptr condition
    std::promise<void> asyncGetPrimaryKeysProm;
    storage->asyncGetPrimaryKeys(testTableInfo, nullptr,
        [&asyncGetPrimaryKeysProm](
            const Error::Ptr& _error, const std::vector<std::string>& _result) {
            BOOST_TEST(_error == nullptr);
            // include [6, 7, 8, 9]
            BOOST_TEST(_result.size() == 100);
            auto valueStr = to_string(0);
            for (auto& v : _result)
            {
                if (v >= valueStr)
                {
                    BOOST_TEST(true);
                    continue;
                }
                BOOST_TEST(false);
            }
            asyncGetPrimaryKeysProm.set_value();
        });

    // add ut for asyncGetRows
    auto keys = make_shared<vector<string>>();
    for (size_t i = 0; i < count; ++i)
    {
        keys->emplace_back(to_string(i));
    }
    std::promise<pair<Error::Ptr, std::map<std::string, Entry::Ptr>>> asyncGetRowsProm;
    storage->asyncGetRows(testTableInfo, keys,
        [&asyncGetRowsProm](
            const Error::Ptr& _error, const std::map<std::string, Entry::Ptr>& _result) {
            asyncGetRowsProm.set_value({_error, _result});
        });
    auto asyncGetRowsRet = asyncGetRowsProm.get_future().get();
    BOOST_TEST(asyncGetRowsRet.first == nullptr);
    BOOST_TEST(asyncGetRowsRet.second.size() == keys->size());
    for (size_t i = 0; i < keys->size(); ++i)
    {
        auto entry = asyncGetRowsRet.second.at(keys->at(i));
        BOOST_TEST(entry != nullptr);
        // BOOST_TEST(entry->getField(testTableKey) == to_string(i));
        BOOST_TEST(entry->getField("value1") == to_string(i + 1));
        BOOST_TEST(entry->getField("value2") == to_string(i + 2));
        BOOST_TEST(entry->num() == 0);
        // BOOST_TEST(entry->dirty() == false);
        BOOST_TEST(entry->getStatus() == Entry::Status::NORMAL);
    }

    // add ut for asyncGetRow
    auto key = 56;
    std::promise<pair<Error::Ptr, Entry::Ptr>> prom;
    storage->asyncGetRow(testTableInfo, to_string(key),
        [&prom](const Error::Ptr& _error, const Entry::Ptr& _result) {
            prom.set_value({_error, _result});
        });
    auto asyncGetRowRet = prom.get_future().get();
    BOOST_TEST(asyncGetRowRet.first == nullptr);
    BOOST_TEST(asyncGetRowRet.second != nullptr);
    // BOOST_TEST(_result->getField(testTableKey) == to_string(i));
    BOOST_TEST(asyncGetRowRet.second->getField("value1") == to_string(key + 1));
    BOOST_TEST(asyncGetRowRet.second->getField("value2") == to_string(key + 2));
    BOOST_TEST(asyncGetRowRet.second->num() == 0);
    // BOOST_TEST(asyncGetRowRet.second->dirty() == false);
    BOOST_TEST(asyncGetRowRet.second->getStatus() == Entry::Status::NORMAL);

    // TODO: add ut for asyncCommitTables
    storage->stop();
}


BOOST_AUTO_TEST_CASE(TableFactory_cache)
{
    storage->start();
    protocol::BlockNumber blockNumber = 0;
    auto infos = vector<TableInfo::Ptr>();
    auto datas = vector<shared_ptr<map<string, Entry::Ptr>>>();
    infos.push_back(testTableInfo);
    auto ret = storage->commitBlock(blockNumber++, infos, datas);
    BOOST_TEST(ret.first == 0);

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
    auto hashImpl = std::make_shared<Sm3Hash>();
    auto tableFactory = make_shared<TableFactory>(storage, hashImpl, blockNumber);
    storage->addStateCache(blockNumber, tableFactory);
    ret = storage->commitBlock(blockNumber++, infos, datas);

    BOOST_TEST(ret.first == count);
    // add ut for addStateCache
    vector<protocol::PBBlock::Ptr> blocks;
    vector<TableFactory::Ptr> tfs;
    auto signImpl = std::make_shared<SM2SignatureImpl>();
    auto cryptoSuite = std::make_shared<CryptoSuite>(hashImpl, signImpl, nullptr);
    auto blockHeaderFactory = std::make_shared<PBBlockHeaderFactory>(cryptoSuite);
    auto transactionFactory = std::make_shared<PBTransactionFactory>(cryptoSuite);
    auto receiptFactory = std::make_shared<PBTransactionReceiptFactory>(cryptoSuite);

    for (size_t i = 0; i < count; ++i)
    {
        auto block =
            make_shared<protocol::PBBlock>(blockHeaderFactory, transactionFactory, receiptFactory);
        blocks.push_back(block);
        tableFactory = make_shared<TableFactory>(storage, hashImpl, i);
        tfs.push_back(tableFactory);
        storage->addStateCache(i, tableFactory);
    }
    for (size_t i = 0; i < count; ++i)
    {  // ut for getBlock
        // auto block = storage->getBlock(i);
        // BOOST_TEST(block == blocks[i]);

        // add ut for getStateCache
        auto tf = storage->getStateCache(i);
        BOOST_TEST(tf == tfs[i]);
    }
    // auto block = storage->getBlock(count + 1);
    // BOOST_TEST(block == nullptr);

    // add ut for getStateCache
    auto tf = storage->getStateCache(count + 1);
    BOOST_TEST(tf == nullptr);

    // ut for dropStateCache
    storage->dropStateCache(count - 1);
    // block = storage->getBlock(count - 1);
    // BOOST_TEST(block == nullptr);

    // add ut for getStateCache
    tf = storage->getStateCache(count - 1);
    BOOST_TEST(tf == nullptr);

    // TODO: add ut for asyncAddStateCache
    // TODO: add ut for asyncDropStateCache
    // TODO: add ut for asyncGetBlock
    // TODO: add ut for asyncGetStateCache
    storage->stop();
}

BOOST_AUTO_TEST_CASE(KVInterfaces)
{
    storage->start();

    // if this ut failed, please `rm -rf unittest_db` and try again
    size_t count = 10;
    for (size_t i = 0; i < count; ++i)
    {
        auto column = to_string(i);
        auto key = to_string(i + 1);
        auto value = to_string(i + 2);
        auto ret = storage->put(column, key, value);
        BOOST_TEST(ret == nullptr);
    }
    for (size_t i = 0; i < count; ++i)
    {
        auto column = to_string(i);
        auto key = to_string(i + 1);
        auto value = to_string(i + 2);
        auto retValue = storage->get(column, key);
        BOOST_TEST(retValue.first == value);
    }

    struct Callback : public std::enable_shared_from_this<Callback>
    {
    public:
        typedef std::shared_ptr<Callback> Ptr;

        explicit Callback(size_t _value) : value(_value) { mutex.lock(); }

        void onResult(
            const Error::Ptr& _error, const std::shared_ptr<std::vector<std::string>>& _result)
        {
            BOOST_TEST(_error == nullptr);
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
        auto pkeys = make_shared<vector<string>>();
        pkeys->push_back(key);
        Callback::Ptr callback = std::make_shared<Callback>(i + 2);
        std::function<void(const Error::Ptr&, const std::shared_ptr<std::vector<std::string>>&)>
            fp = std::bind(
                &Callback::onResult, callback, std::placeholders::_1, std::placeholders::_2);
        storage->asyncGetBatch(column, pkeys, fp);
        // lock to wait for async send
        callback->mutex.lock();
        callback->mutex.unlock();
    }
    storage->stop();
}

BOOST_AUTO_TEST_SUITE_END()
}  // namespace test
}  // namespace bcos
