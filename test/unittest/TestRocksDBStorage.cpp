#include "../../bcos-storage/RocksDBStorage.h"
#include "bcos-framework/libtable/TableStorage.h"
#include "boost/filesystem.hpp"
#include "interfaces/storage/StorageInterface.h"
#include <tbb/concurrent_vector.h>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

using namespace bcos::storage;

namespace bcos::test
{
struct TestRocksDBStorageFixture
{
    TestRocksDBStorageFixture()
    {
        rocksdb::DB* db;
        rocksdb::Options options;
        // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
        options.IncreaseParallelism();
        options.OptimizeLevelStyleCompaction();
        // create the DB if it's not already present
        options.create_if_missing = true;

        // open DB
        rocksdb::Status s = rocksdb::DB::Open(options, path, &db);
        BOOST_CHECK_EQUAL(s.ok(), true);

        rocksDBStorage = std::make_shared<RocksDBStorage>(std::unique_ptr<rocksdb::DB>(db));
    }

    void prepareNonTableData()
    {
        for (size_t i = 0; i < 1000; ++i)
        {
            std::string key = "key" + boost::lexical_cast<std::string>(i);
            auto entry = std::make_shared<Entry>();
            entry->importFields({"value_" + boost::lexical_cast<std::string>(i), "value1", "value2",
                "value3", "value4", "value5"});

            rocksDBStorage->asyncSetRow(nullptr, key, entry, [](Error::Ptr&& error, bool success) {
                BOOST_CHECK_EQUAL(error, nullptr);
                BOOST_CHECK_EQUAL(success, true);
            });
        }
    }

    void cleanupNonTableData()
    {
        for (size_t i = 0; i < 1000; ++i)
        {
            std::string key = "key" + boost::lexical_cast<std::string>(i);
            auto entry = std::make_shared<Entry>();
            entry->setStatus(Entry::DELETED);

            rocksDBStorage->asyncSetRow(nullptr, key, entry, [](Error::Ptr&& error, bool success) {
                BOOST_CHECK_EQUAL(error, nullptr);
                BOOST_CHECK_EQUAL(success, true);
            });
        }
    }

    ~TestRocksDBStorageFixture()
    {
        if (boost::filesystem::exists(path))
        {
            boost::filesystem::remove_all(path);
        }
    }

    std::string path = "./unittestdb";
    RocksDBStorage::Ptr rocksDBStorage;
};
BOOST_FIXTURE_TEST_SUITE(TestRocksDBStorage, TestRocksDBStorageFixture)

BOOST_AUTO_TEST_CASE(asyncGetRow)
{
    prepareNonTableData();

    tbb::concurrent_vector<std::function<void()>> checks;
    tbb::parallel_for(
        tbb::blocked_range<size_t>(0, 1050), [&](const tbb::blocked_range<size_t>& range) {
            for (size_t i = range.begin(); i != range.end(); ++i)
            {
                std::string key = "key" + boost::lexical_cast<std::string>(i);
                rocksDBStorage->asyncGetRow(
                    nullptr, key, [&](Error::Ptr&& error, Entry::Ptr&& entry) {
                        checks.push_back([i, error, entry]() {
                            BOOST_CHECK_EQUAL(error, nullptr);
                            if (i < 1000)
                            {
                                BOOST_CHECK_NE(entry, nullptr);

                                auto& data = entry->fields();
                                auto fields = std::vector<std::string>(
                                    {"value_" + boost::lexical_cast<std::string>(i), "value1",
                                        "value2", "value3", "value4", "value5"});
                                BOOST_CHECK_EQUAL_COLLECTIONS(
                                    data.begin(), data.end(), fields.begin(), fields.end());
                            }
                            else
                            {
                                BOOST_CHECK_EQUAL(entry, nullptr);
                            }
                        });
                    });
            }
        });

    for (auto& it : checks)
    {
        it();
    }

    cleanupNonTableData();
}

BOOST_AUTO_TEST_CASE(asyncGetPrimaryKeys)
{
    prepareNonTableData();
    rocksDBStorage->asyncGetPrimaryKeys(
        nullptr, nullptr, [&](Error::Ptr&& error, std::vector<std::string>&& keys) {
            BOOST_CHECK_EQUAL(error, nullptr);
            BOOST_CHECK_EQUAL(keys.size(), 1000);

            std::vector<std::string> sortedKeys;

            for (size_t i = 0; i < 1000; ++i)
            {
                sortedKeys.emplace_back("key" + boost::lexical_cast<std::string>(i));
            }

            std::sort(sortedKeys.begin(), sortedKeys.end());

            BOOST_CHECK_EQUAL_COLLECTIONS(
                sortedKeys.begin(), sortedKeys.end(), keys.begin(), keys.end());
        });

    auto tableInfo = std::make_shared<TableInfo>("new_table", "key", "value");

    for (size_t i = 1000; i < 2000; ++i)
    {
        std::string key = "newkey" + boost::lexical_cast<std::string>(i);
        auto entry = std::make_shared<Entry>(tableInfo, 0);
        entry->importFields({"value12345"});

        rocksDBStorage->asyncSetRow(tableInfo, key, entry, [&](Error::Ptr&& error, bool success) {
            BOOST_CHECK_EQUAL(error, nullptr);
            BOOST_CHECK_EQUAL(success, true);
        });
    }

    // query old data
    auto condition = std::make_shared<Condition>();
    rocksDBStorage->asyncGetPrimaryKeys(
        tableInfo, condition, [](Error::Ptr&& error, std::vector<std::string>&& keys) {
            BOOST_CHECK_EQUAL(error, nullptr);
            BOOST_CHECK_EQUAL(keys.size(), 1000);

            std::vector<std::string> sortedKeys;

            for (size_t i = 0; i < 1000; ++i)
            {
                sortedKeys.emplace_back("newkey" + boost::lexical_cast<std::string>(i + 1000));
            }
            std::sort(sortedKeys.begin(), sortedKeys.end());

            BOOST_CHECK_EQUAL_COLLECTIONS(
                sortedKeys.begin(), sortedKeys.end(), keys.begin(), keys.end());
        });

    // re-query non table data
    rocksDBStorage->asyncGetPrimaryKeys(
        nullptr, nullptr, [&](Error::Ptr&& error, std::vector<std::string>&& keys) {
            BOOST_CHECK_EQUAL(error, nullptr);
            BOOST_CHECK_EQUAL(keys.size(), 1000);

            std::vector<std::string> sortedKeys;

            for (size_t i = 0; i < 1000; ++i)
            {
                sortedKeys.emplace_back("key" + boost::lexical_cast<std::string>(i));
            }

            std::sort(sortedKeys.begin(), sortedKeys.end());

            BOOST_CHECK_EQUAL_COLLECTIONS(
                sortedKeys.begin(), sortedKeys.end(), keys.begin(), keys.end());
        });

    rocksDBStorage->asyncGetRow(tableInfo, "newkey" + boost::lexical_cast<std::string>(1050),
        [&](Error::Ptr&& error, Entry::Ptr&& entry) {
            BOOST_CHECK_EQUAL(error, nullptr);
            BOOST_CHECK_NE(entry, nullptr);
        });

    // clean new data
    for (size_t i = 0; i < 1000; ++i)
    {
        std::string key = "newkey" + boost::lexical_cast<std::string>(i + 1000);
        auto entry = std::make_shared<Entry>();
        entry->setStatus(Entry::DELETED);

        rocksDBStorage->asyncSetRow(tableInfo, key, entry, [](Error::Ptr&& error, bool success) {
            BOOST_CHECK_EQUAL(error, nullptr);
            BOOST_CHECK_EQUAL(success, true);
        });
    }

    rocksDBStorage->asyncGetRow(tableInfo, "newkey" + boost::lexical_cast<std::string>(1050),
        [&](Error::Ptr&& error, Entry::Ptr&& entry) {
            BOOST_CHECK_EQUAL(error, nullptr);
            BOOST_CHECK_EQUAL(entry, nullptr);
        });

    // check if the data is deleted
    rocksDBStorage->asyncGetPrimaryKeys(
        tableInfo, nullptr, [](Error::Ptr&& error, std::vector<std::string>&& keys) {
            BOOST_CHECK_EQUAL(error, nullptr);
            BOOST_CHECK_EQUAL(keys.size(), 0);
        });

    cleanupNonTableData();
}

BOOST_AUTO_TEST_CASE(asyncGetRows)
{
    prepareNonTableData();

    std::vector<std::string> keys;
    for (size_t i = 0; i < 1050; ++i)
    {
        std::string key = "key" + boost::lexical_cast<std::string>(i);
        keys.push_back(key);
    }
    rocksDBStorage->asyncGetRows(
        nullptr, keys, [&](Error::Ptr&& error, std::vector<Entry::Ptr>&& entries) {
            BOOST_CHECK_EQUAL(error, nullptr);
            BOOST_CHECK_EQUAL(entries.size(), 1050);

            for (size_t i = 0; i < 1050; ++i)
            {
                auto& entry = entries[i];
                if (i < 1000)
                {
                    BOOST_CHECK_NE(entry, nullptr);
                    auto& data = entry->fields();
                    auto fields =
                        std::vector<std::string>({"value_" + boost::lexical_cast<std::string>(i),
                            "value1", "value2", "value3", "value4", "value5"});
                    BOOST_CHECK_EQUAL_COLLECTIONS(
                        data.begin(), data.end(), fields.begin(), fields.end());
                }
                else
                {
                    BOOST_CHECK_EQUAL(entry, nullptr);
                }
            }
        });

    cleanupNonTableData();
}

class Header256Hash : public bcos::crypto::Hash
{
public:
    typedef std::shared_ptr<Header256Hash> Ptr;
    Header256Hash() = default;
    virtual ~Header256Hash(){};
    bcos::crypto::HashType hash(bytesConstRef _data) override
    {
        std::hash<std::string_view> hash;
        return bcos::crypto::HashType(
            hash(std::string_view((const char*)_data.data(), _data.size())));
    }
};

BOOST_AUTO_TEST_CASE(asyncPrepare)
{
    prepareNonTableData();

    auto hashImpl = std::make_shared<Header256Hash>();
    auto storage = std::make_shared<bcos::storage::TableStorage>(nullptr, hashImpl, 0);
    BOOST_CHECK_EQUAL(storage->createTable("table1", "key", "value1,value2,value3"), true);
    BOOST_CHECK_EQUAL(
        storage->createTable("table2", "key", "value1,value2,value3,value4,value5"), true);

    auto table1 = storage->openTable("table1");
    auto table2 = storage->openTable("table2");

    BOOST_CHECK_NE(table1, nullptr);
    BOOST_CHECK_NE(table2, nullptr);

    std::vector<std::string> table1Keys;
    std::vector<std::string> table2Keys;

    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = table1->newEntry();
        auto key1 = "key" + boost::lexical_cast<std::string>(i);
        entry->setField("value1", "hello world!" + boost::lexical_cast<std::string>(i));
        table1->setRow(key1, entry);
        table1Keys.push_back(key1);

        auto entry2 = table2->newEntry();
        auto key2 = "key" + boost::lexical_cast<std::string>(i);
        entry2->setField("value3", "hello world!" + boost::lexical_cast<std::string>(i));
        table2->setRow(key2, entry2);
        table2Keys.push_back(key2);
    }

    rocksDBStorage->asyncPrepare(bcos::storage::TransactionalStorageInterface::PrepareParams(),
        storage, [&](Error::Ptr&& error) { BOOST_CHECK_EQUAL(error, nullptr); });

    rocksDBStorage->asyncGetPrimaryKeys(
        table1->tableInfo(), nullptr, [&](Error::Ptr&& error, std::vector<std::string>&& keys) {
            BOOST_CHECK_EQUAL(error, nullptr);
            BOOST_CHECK_EQUAL(keys.size(), 10);

            std::sort(table1Keys.begin(), table1Keys.end());
            BOOST_CHECK_EQUAL_COLLECTIONS(
                table1Keys.begin(), table1Keys.end(), keys.begin(), keys.end());

            rocksDBStorage->asyncGetRows(table1->tableInfo(), table1Keys,
                [&](Error::Ptr&& error, std::vector<Entry::Ptr>&& entries) {
                    BOOST_CHECK_EQUAL(error, nullptr);
                    BOOST_CHECK_EQUAL(entries.size(), 10);

                    for (size_t i = 0; i < 10; ++i)
                    {
                        BOOST_CHECK_EQUAL(entries[i]->getField("value1"),
                            std::string("hello world!") + table1Keys[i][3]);
                    }
                });
        });

    rocksDBStorage->asyncGetPrimaryKeys(
        table2->tableInfo(), nullptr, [&](Error::Ptr&& error, std::vector<std::string>&& keys) {
            BOOST_CHECK_EQUAL(error, nullptr);
            BOOST_CHECK_EQUAL(keys.size(), 10);

            std::sort(table2Keys.begin(), table2Keys.end());
            BOOST_CHECK_EQUAL_COLLECTIONS(
                table2Keys.begin(), table2Keys.end(), keys.begin(), keys.end());

            rocksDBStorage->asyncGetRows(table2->tableInfo(), table2Keys,
                [&](Error::Ptr&& error, std::vector<Entry::Ptr>&& entries) {
                    BOOST_CHECK_EQUAL(error, nullptr);
                    BOOST_CHECK_EQUAL(entries.size(), 10);

                    for (size_t i = 0; i < 10; ++i)
                    {
                        BOOST_CHECK_EQUAL(entries[i]->getField("value3"),
                            std::string("hello world!") + table2Keys[i][3]);
                    }
                });
        });

    cleanupNonTableData();
}

BOOST_AUTO_TEST_CASE(boostSerialize)
{
    // encode the vector
    std::vector<std::string> forEncode(5);
    forEncode[3] = "hello world!";

    std::string buffer;
    boost::iostreams::stream<boost::iostreams::back_insert_device<std::string>> outputStream(
        buffer);
    boost::archive::binary_oarchive archive(outputStream,
        boost::archive::no_header | boost::archive::no_codecvt | boost::archive::no_tracking);

    archive << forEncode;
    outputStream.flush();

    std::cout << forEncode << std::endl;

    // decode the vector
    boost::iostreams::stream<boost::iostreams::array_source> inputStream(
        buffer.data(), buffer.size());
    boost::archive::binary_iarchive archive2(inputStream,
        boost::archive::no_header | boost::archive::no_codecvt | boost::archive::no_tracking);

    std::vector<std::string> forDecode;
    archive2 >> forDecode;

    std::cout << forDecode;

    BOOST_CHECK_EQUAL_COLLECTIONS(
        forEncode.begin(), forEncode.end(), forDecode.begin(), forDecode.end());
}

BOOST_AUTO_TEST_SUITE_END()

}  // namespace bcos::test