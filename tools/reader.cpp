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
 * @file reader.cpp
 * @author: xingqiangbai
 * @date 2020-06-29
 */

#include "RocksDBStorage.h"
#include "bcos-framework/libtable/TableStorage.h"
#include "boost/filesystem.hpp"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/exception/diagnostic_information.hpp>
#include <boost/program_options.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/throw_exception.hpp>
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
    main_options.add_options()("help,h", "help of Table benchmark")(
        "path,p", po::value<string>()->default_value(""), "[RocksDB path]")("name,n",
        po::value<string>()->default_value(""), "[RocksDB name]")("table,t", po::value<string>(),
        "table name ")("key,k", po::value<string>()->default_value(""), "table key")(
        "iterate,i", po::value<bool>()->default_value(false), "traverse table");
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
    auto storageName = params["name"].as<string>();
    if (!fs::exists(storagePath))
    {
        cout << "the path is empty:" << storagePath << endl;
        return 0;
    }
    auto iterate = params["iterate"].as<bool>();
    auto tableName = params["table"].as<string>();
    auto key = params["key"].as<string>();

    cout << "rocksdb path : " << storagePath << endl;
    cout << "tableName    : " << tableName << endl;
    // auto factory = make_shared<RocksDBAdapterFactory>(storagePath);

    rocksdb::DB* db;
    rocksdb::Options options;
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    options.create_if_missing = false;
    rocksdb::Status s = rocksdb::DB::Open(options, storagePath, &db);

    auto adapter = std::make_shared<RocksDBStorage>(std::unique_ptr<rocksdb::DB>(db));

    auto sysTableInfo = std::make_shared<storage::TableInfo>(tableName, TableStorage::SYS_TABLE_KEY,
        std::string(TableStorage::SYS_TABLE_KEY_FIELDS) + "," +
            TableStorage::SYS_TABLE_VALUE_FIELDS);
    ;
    TableInfo::Ptr tableInfo = sysTableInfo;
    if (tableName != TableStorage::SYS_TABLES)
    {
        std::promise<Entry::Ptr> entryPromise;
        adapter->asyncGetRow(sysTableInfo, tableName, [&](Error::Ptr&& error, Entry::Ptr&& entry) {
            if (error)
            {
                BOOST_THROW_EXCEPTION(*error);
            }
            entryPromise.set_value(std::move(entry));
        });

        auto entry = entryPromise.get_future().get();

        if (!entry)
        {
            cout << tableName << " doesn't exist in DB:" << storagePath + "/" + storageName << endl;
            exit(1);
        }

        tableInfo =
            make_shared<TableInfo>(tableName, entry->getField(TableStorage::SYS_TABLE_KEY_FIELDS),
                entry->getField(TableStorage::SYS_TABLE_VALUE_FIELDS));
    }
    if (iterate)
    {
        cout << "iterator " << tableInfo->name << endl;

        std::promise<std::vector<std::string>> keysPromise;
        adapter->asyncGetPrimaryKeys(
            tableInfo, nullptr, [&](Error::Ptr&& error, std::vector<std::string>&& keys) {
                if (error)
                {
                    BOOST_THROW_EXCEPTION(*error);
                }

                keysPromise.set_value(std::move(keys));
            });

        auto keys = keysPromise.get_future().get();

        if (keys.empty())
        {
            cout << tableName << " is empty" << endl;
            return 0;
        }
        // cout << "keys=" << boost::algorithm::join(keys, "\t") << endl;
        for (auto& k : keys)
        {
            cout << "key=" << k << "|";

            std::promise<Entry::Ptr> rowPromise;
            adapter->asyncGetRow(tableInfo, k, [&](Error::Ptr&& error, Entry::Ptr&& entry) {
                if (error)
                {
                    BOOST_THROW_EXCEPTION(*error);
                }

                rowPromise.set_value(std::move(entry));
            });
            auto row = rowPromise.get_future().get();

            for (auto& it : *row)
            {
                cout << " [" << it << "] ";
            }
            cout << " [status=" << row->status() << "]"
                 << " [num=" << row->num() << "]";
            cout << endl;
        }
        return 0;
    }
    std::promise<Entry::Ptr> rowPromise;
    adapter->asyncGetRow(tableInfo, key, [&](Error::Ptr&& error, Entry::Ptr&& entry) {
        if (error)
        {
            BOOST_THROW_EXCEPTION(*error);
        }

        rowPromise.set_value(std::move(entry));
    });
    auto row = rowPromise.get_future().get();
    for (auto& it : *row)
    {
        cout << "[" << it << "]";
    }
    cout << " [status=" << row->status() << "]"
         << " [num=" << row->num() << "]" << endl;
    return 0;
}
