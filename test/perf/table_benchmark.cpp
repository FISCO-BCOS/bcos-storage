/**
 * @CopyRight:
 * FISCO-BCOS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * FISCO-BCOS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with FISCO-BCOS.  If not, see <http://www.gnu.org/licenses/>
 * (c) 2016-2018 fisco-dev contributors.
 *
 * @file table_benchmark.cpp
 * @author: xingqiangbai
 * @date 2020-03-18
 */

#include "KVDBImpl.h"
#include "RocksDBAdapter/RocksDBAdapter.h"
#include "RocksDBAdapter/RocksDBAdapterFactory.h"
#include "Storage.h"
#include "bcos-framework/libtable/TableFactory.h"
#include "bcos-framework/testutils/HashImpl.h"
#include "boost/filesystem.hpp"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/program_options.hpp>
#include <boost/property_tree/ptree.hpp>
#include <cstdlib>
#include <functional>

using namespace std;
using namespace bcos;
using namespace bcos::test;
using namespace bcos::storage;

namespace fs = boost::filesystem;
namespace po = boost::program_options;

po::options_description main_options("Main for Table benchmark");

po::variables_map initCommandLine(int argc, const char* argv[])
{
    main_options.add_options()("help,h", "help of Table benchmark")(
        "path,p", po::value<string>()->default_value("benchmark/table/"), "[RocksDB path]")(
        "keys,k", po::value<int>()->default_value(10000), "the number of different keys")("value,v",
        po::value<int>()->default_value(256),
        "the length of value")("random,r", "every test use a new rocksdb")(
        "iterate,i", po::value<bool>()->default_value(false), "traverse rocksdb");
    po::variables_map vm;
    try
    {
        po::store(po::parse_command_line(argc, argv, main_options), vm);
        po::notify(vm);
    }
    catch (...)
    {
        std::cout << "invalid input" << std::endl;
        exit(0);
    }
    if (vm.count("help") || vm.count("h"))
    {
        std::cout << main_options << std::endl;
        exit(0);
    }
    return vm;
}

int main(int argc, const char* argv[])
{
    boost::property_tree::ptree pt;
    auto params = initCommandLine(argc, argv);
    auto storagePath = params["path"].as<string>();
    if (params.count("random"))
    {
        storagePath += to_string(utcTime());
    }
    if (fs::exists(storagePath))
    {
        fs::remove_all(storagePath);
    }
    auto keys = params["keys"].as<int>();
    auto valueLength = params["value"].as<int>();
    auto iterate = params["iterate"].as<bool>();
    int tables = 100;
    int64_t blockNumber = 0;
    if (keys % tables != 0)
    {
        keys = (keys + tables) / tables * tables;
    }

    cout << "rocksdb path    : " << storagePath << endl;
    cout << "value length(B) : " << valueLength << endl;
    cout << "iterate         : " << iterate << endl;
    cout << "number of KV    : " << keys << endl;
    auto factory = make_shared<RocksDBAdapterFactory>(storagePath);
    if (iterate)
    {
        vector<string> columnFamilies{METADATA_COLUMN_NAME};
        auto ret = factory->createRocksDB(
            "state", RocksDBAdapter::TABLE_PERFIX_LENGTH, true, columnFamilies);
        assert(ret.first);
        auto db = ret.first;
        auto it = db->NewIterator(rocksdb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next())
        {
            cout << it->key().ToString() << ": " << it->value().ToString() << endl;
        }
        delete it;
        return 0;
    }
    auto ret = factory->createRocksDB("KV_test", RocksDBAdapter::TABLE_PERFIX_LENGTH);
    auto kvDB = make_shared<KVDBImpl>(ret.first);
    auto adapter = factory->createAdapter("state", RocksDBAdapter::TABLE_PERFIX_LENGTH);
    auto storage = make_shared<StorageImpl>(adapter, kvDB);
    auto hashImpl = std::make_shared<Keccak256Hash>();
    auto tableFactory = make_shared<TableFactory>(storage, hashImpl, blockNumber);

    auto commitData = [&](int64_t block) {
        tableFactory->commit();
        tableFactory = make_shared<TableFactory>(storage, hashImpl, block);
    };
    // commitData(0);


    auto performance = [&](const string& description, int count, std::function<void()> operation) {
        auto now = std::chrono::steady_clock::now();
        // cout << "<<<<<<<<<< " << description << endl;
        operation();
        std::chrono::duration<double> elapsed = std::chrono::steady_clock::now() - now;
        cout << "<<<<<<<<<< " << description
             << "|time used(s)=" << std::setiosflags(std::ios::fixed) << std::setprecision(3)
             << elapsed.count() << " rounds=" << count << " tps=" << count / elapsed.count() << "|"
             << endl;
        now = std::chrono::steady_clock::now();
        commitData(blockNumber++);
        elapsed = std::chrono::steady_clock::now() - now;
        cout << "<<<<<<<<<< "
             << "commit time(s)=" << elapsed.count() << endl;
    };


    auto createTable = [&](const string& prefix, int count) {
        for (int i = 0; i < count; ++i)
        {
            string keyField("key");
            string valueFields("value,value2");
            tableFactory->createTable(prefix + to_string(i), keyField, valueFields);
        }
    };

    auto insert = [&](const string& prefix, const string& value, int count) {
        auto keysPerTable = count / tables;
        tbb::parallel_for(
            tbb::blocked_range<size_t>(0, tables), [&](const tbb::blocked_range<size_t>& range) {
                for (auto it = range.begin(); it != range.end(); ++it)
                {
                    auto tableName = prefix + to_string(it);
                    auto table = tableFactory->openTable(tableName);
                    for (int i = 0; i < keysPerTable; ++i)
                    {
                        auto entry = table->newEntry();
                        entry->setField("key", to_string(i));
                        entry->setField("value", value);
                        entry->setField("value2", value + "2");
                        table->setRow(to_string(i), entry);
                    }
                }
            });
    };

    auto select = [&](const string& prefix, int count) {
        auto keysPerTable = count / tables;
        tbb::parallel_for(
            tbb::blocked_range<size_t>(0, tables), [&](const tbb::blocked_range<size_t>& range) {
                for (auto it = range.begin(); it != range.end(); ++it)
                {
                    auto tableName = prefix + to_string(it);
                    auto table = tableFactory->openTable(tableName);

                    for (int i = 0; i < keysPerTable; ++i)
                    {
                        auto entry = table->getRow(to_string(i));
#if 0
                        if (entry != nullptr)
                        {
                            cout << "key:" << i << ",value:" << entry->getFieldConst("value").size()
                                 << endl;
                        }
                        else
                        {
                            cout << "empty key:" << i << endl;
                        }
#endif
                    }
                }
            });
    };

    auto traverse = [&](const string& prefix, int count) {
        auto keysPerTable = count / tables;
        tbb::parallel_for(
            tbb::blocked_range<size_t>(0, tables), [&](const tbb::blocked_range<size_t>& range) {
                for (auto it = range.begin(); it != range.end(); ++it)
                {
                    auto tableName = prefix + to_string(it);
                    auto table = tableFactory->openTable(tableName);
                    vector<string> queryKeys;
                    for (int i = 0; i < keysPerTable; ++i)
                    {
                        queryKeys.emplace_back(to_string(i));
                    }
                    auto entries = table->getRows(queryKeys);
#if 0
                    for (auto& item : entries)
                    {
                        auto entry = item.second;
                        if (entry != nullptr)
                        {
                            cout << "key:" << item.first << ",value:" << entry->getFieldConst("value").size()
                            << endl;
                        }
                        else
                        {
                            cout << "empty key:" << item.first << endl;
                        }
                    }
#endif
                }
            });
    };

    auto remove = [&](const string& tableName, int count) {
        auto table = tableFactory->openTable(tableName);
        for (int i = 0; i < count; ++i)
        {
            table->remove(to_string(i));
        }
    };
    cout << "<<<<<<<<<< " << endl;
    string value;
    value.resize(valueLength);
    for (int i = 0; i < valueLength; ++i)
    {
        value[i] = '0' + rand() % 10;
    }
    performance("create Table", tables, [&]() { createTable("table", tables); });
    string testTableName("table0");
    performance("Table set", keys, [&]() { insert("table", value, keys); });
    performance("Table get", keys, [&]() { select("table", keys); });
    performance("Table traverse", keys, [&]() { traverse("table", keys); });
    // performance("Table remove", keys, [&]() { remove(testTableName, keys); });

    string bigValue;
    for (int i = 0; i < keys; ++i)
    {
        bigValue.append(value);
    }
    auto bigValueSet = [&](const string& tableName) {
        auto table = tableFactory->openTable(tableName);

        cout << "bigValue size:" << bigValue.size() / 1024 << "KB" << endl;
        auto entry = table->newEntry();
        entry->setField("key", "bigValue");
        entry->setField("value", bigValue);
        entry->setField("value2", "2");
        table->setRow("bigValue", entry);
    };
    auto bigValueGet = [&](const string& tableName) {
        auto table = tableFactory->openTable(tableName);
        auto entry = table->getRow("bigValue");
        auto bigV = entry->getFieldConst("value");
        cout << "bigValue size:" << bigV.size() / 1024 << "KB" << endl;
    };
    performance("Big Value set", keys, [&]() { bigValueSet("table1"); });
    performance("Big Value get", keys, [&]() { bigValueGet("table1"); });

    return 0;
}
