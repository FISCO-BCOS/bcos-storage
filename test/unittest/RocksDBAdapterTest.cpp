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
 * @brief the test of rocksdb adapter
 * @file RocksDBAdapterTest.cpp
 */

#include "bcos-storage/RocksDBAdapter.h"
#include "bcos-framework/libtable/TableFactory.h"
#include "bcos-storage/RocksDBAdapterFactory.h"
#include "rocksdb/db.h"
#include <boost/test/unit_test.hpp>
#include "boost/filesystem.hpp"

using namespace std;
using namespace rocksdb;
using namespace bcos;
using namespace bcos::storage;
namespace fs = boost::filesystem;

namespace bcos
{
namespace test
{
struct RocksDBAdapterFixture
{
    RocksDBAdapterFixture()
    {
        factory = make_shared<RocksDBAdapterFactory>(testPath.string());
        adapter = factory->createAdapter("test_db_1", RocksDBAdapter::TABLE_PREFIX_LENGTH);
        testTableInfo =
            std::make_shared<storage::TableInfo>(testTableName, testTableKey, "value1,value2");
        testTableInfo->newTable = true;
    }
    ~RocksDBAdapterFixture()
    {
        adapter.reset();
        if (fs::exists(testPath))
        {
            fs::remove_all(testPath);
        }
    }
    fs::path testPath = "./unittest_db";
    std::shared_ptr<RocksDBAdapter> adapter = nullptr;
    std::shared_ptr<RocksDBAdapterFactory> factory = nullptr;
    std::shared_ptr<TableInfo> testTableInfo = nullptr;
    string testTableName = "t_test";
    string testTableKey = "key";
};
BOOST_FIXTURE_TEST_SUITE(RocksDBAdapterTest, RocksDBAdapterFixture)

BOOST_AUTO_TEST_CASE(commitTables)
{
    // if this ut failed, please `rm -rf unittest_db` and try again
    auto infos = vector<TableInfo::Ptr>();
    auto datas = vector<shared_ptr<map<string, Entry::Ptr>>>();
    infos.push_back(testTableInfo);
    auto ret = adapter->commitTables(infos, datas);
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

    // mock create table
    auto sysTableInfo = getSysTableInfo(SYS_TABLE);
    infos.push_back(sysTableInfo);
    auto sysTableData = make_shared<map<string, Entry::Ptr>>();
    entry = make_shared<Entry>();
    entry->setField(SYS_TABLE_KEY, testTableInfo->name);
    (*sysTableData)[testTableInfo->name] = entry;

    datas.push_back(sysTableData);
    ret = adapter->commitTables(infos, datas);
    BOOST_TEST(ret.first == count + 2); // sys table + deleted

    for (size_t i = 0; i < count; ++i)
    {
        auto entry = adapter->getRow(testTableInfo, to_string(i));
        BOOST_TEST(entry != nullptr);
        BOOST_TEST(entry->getField("value1") == to_string(i + 1));
        BOOST_TEST(entry->getField("value2") == to_string(i + 2));
        BOOST_TEST(entry->getField(testTableKey) == to_string(i));
        BOOST_TEST(entry->num() == 0);
        BOOST_TEST(entry->dirty() == false);
        BOOST_TEST(entry->getStatus() == Entry::Status::NORMAL);
    }

    auto keys = adapter->getPrimaryKeys(testTableInfo, nullptr);
    BOOST_TEST(keys.size() == count);
    auto entries = adapter->getRows(testTableInfo, keys);
    BOOST_TEST(entries.size() == count);
    for (size_t i = 0; i < count; ++i)
    {
        auto entry = entries[to_string(i)];
        BOOST_TEST(entry != nullptr);
        BOOST_TEST(entry->getField(testTableKey) == to_string(i));
        BOOST_TEST(entry->getField("value1") == to_string(i + 1));
        BOOST_TEST(entry->getField("value2") == to_string(i + 2));
        BOOST_TEST(entry->num() == 0);
        BOOST_TEST(entry->dirty() == false);
        BOOST_TEST(entry->getStatus() == Entry::Status::NORMAL);
    }
}

BOOST_AUTO_TEST_SUITE_END()
}  // namespace test
}  // namespace bcos
