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
#include "RocksDBAdapter.h"
#include "RocksDBAdapterFactory.h"
#include "boost/filesystem.hpp"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/program_options.hpp>
#include <boost/property_tree/ptree.hpp>
#include <cstdlib>
#include <functional>

using namespace std;
using namespace rocksdb;
using namespace bcos;
using namespace bcos::storage;

namespace fs = boost::filesystem;
namespace po = boost::program_options;

po::options_description main_options("Main for Table benchmark");

po::variables_map initCommandLine(int argc, const char* argv[])
{
    main_options.add_options()("help,h", "help of Table benchmark")("path,p",
        po::value<string>()->default_value("benchmark/rocksDB/"),
        "[RocksDB path]")("count,c", po::value<int>()->default_value(10000),
        "the number of different keys")("value,v", po::value<int>()->default_value(256),
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
    auto count = params["count"].as<int>();
    auto valueLength = params["value"].as<int>();
    auto iterate = params["iterate"].as<bool>();

    cout << "rocksdb path    : " << storagePath << endl;
    cout << "value length(B) : " << valueLength << endl;
    cout << "iterate         : " << iterate << endl;
    cout << "number of KV    : " << count << endl;
    auto factory = make_shared<RocksDBAdapterFactory>(storagePath);
    string dbName = "rocksdb";
    auto ret = factory->createRocksDB(dbName, 0);
    auto db = std::shared_ptr<rocksdb::DB>(ret.first);
    assert(db);
    if (iterate)
    {
        auto it = db->NewIterator(rocksdb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next())
        {
            cout << it->key().ToString() << ": " << it->value().ToString() << endl;
        }
        delete it;
        return 0;
    }

    vector<string> rocksKeys;
    for (int i = 0; i < count; ++i)
    {
        rocksKeys.emplace_back(to_string(i));
    }
    auto insert = [&](const string& value, int count) {
        rocksdb::WriteBatch writeBatch;
        for (int i = 0; i < count; ++i)
        {
            writeBatch.Put(
                rocksdb::Slice(rocksKeys[i]), rocksdb::Slice(value.data(), value.size()));
        }
        db->Write(rocksdb::WriteOptions(), &writeBatch);
    };

    auto get = [&]() {
        for (auto& key : rocksKeys)
        {
            string ret;
            db->Get(rocksdb::ReadOptions(), Slice(key), &ret);
        }
    };

    vector<Slice> rocksSliceKeys;
    for (int i = 0; i < count; ++i)
    {
        rocksSliceKeys.emplace_back(rocksKeys[i]);
    }
    auto multiGet = [&]() {
        vector<string> values;
        values.reserve(rocksKeys.size());
        db->MultiGet(rocksdb::ReadOptions(), rocksSliceKeys, &values);
    };
    auto performance = [&](const string& description, int count, std::function<void()> operation) {
        auto now = std::chrono::steady_clock::now();
        operation();
        std::chrono::duration<double> elapsed = std::chrono::steady_clock::now() - now;
        cout << "<<<<<<<<<< " << description
             << "|time used(s)=" << std::setiosflags(std::ios::fixed) << std::setprecision(3)
             << elapsed.count() << " rounds=" << count << " tps=" << count / elapsed.count() << "|"
             << endl;
    };


    string value;
    value.resize(valueLength);
    for (int i = 0; i < valueLength; ++i)
    {
        value[i] = '0' + rand() % 10;
    }
    performance("RocksDB batch set", count, [&]() { insert(value, count); });
    performance("RocksDB get", count, [&]() { get(); });
    performance("RocksDB multi get", count, [&]() { multiGet(); });

    ret = factory->createRocksDB("kv", 0);
    auto kvDB = make_shared<KVDBImpl>(ret.first);
    auto kvSet = [&](const string& value, int count) {
        for (int i = 0; i < count; ++i)
        {
            kvDB->put("default", rocksKeys[i], value);
        }
    };

    auto kvGet = [&]() {
        for (auto& key : rocksKeys)
        {
            kvDB->get("default", key);
        }
    };
    performance("KV single put", count, [&]() { kvSet(value, count); });
    performance("KV get", count, [&]() { kvGet(); });
    string bigValueKey("bigValue");
    string bigValue;
    for (int i = 0; i < count; ++i)
    {
        bigValue.append(value);
    }
    cout << "bigValue size:" << bigValue.size() / 1024 << "KB" << endl;
    auto bigValueSet = [&]() {
        db->Put(rocksdb::WriteOptions(), rocksdb::Slice(bigValueKey), rocksdb::Slice(bigValue));
    };

    auto bigValueGet = [&]() {
        string ret;
        db->Get(rocksdb::ReadOptions(), rocksdb::Slice(bigValueKey), &ret);
        cout << "get bigValue size:" << ret.size() / 1024 << "KB" << endl;
    };

    performance("Big Value set", count, [&]() { bigValueSet(); });
    performance("Big Value get", count, [&]() { bigValueGet(); });

    return 0;
}
