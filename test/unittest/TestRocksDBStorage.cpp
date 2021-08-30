#include "../../bcos-storage/RocksDBStorage.h"
#include "boost/filesystem.hpp"
#include <boost/lexical_cast.hpp>
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

    /*
    tbb::parallel_for(
        tbb::blocked_range<size_t>(0, 1050), [&](const tbb::blocked_range<size_t>& range) {
            for (size_t i = range.begin(); i != range.end(); ++i)
            {
                std::string key = "key" + boost::lexical_cast<std::string>(i);
                rocksDBStorage->asyncGetRow(
                    nullptr, key, [&](Error::Ptr&& error, Entry::Ptr&& entry) {
                        BOOST_CHECK_EQUAL(error, nullptr);
                        BOOST_CHECK_NE(entry, nullptr);

                        auto& data = entry->fields();
                        auto fields = std::vector<std::string>(
                            {"value_" + boost::lexical_cast<std::string>(i), "value1", "value2",
                                "value3", "value4", "value5"});
                        BOOST_CHECK_EQUAL_COLLECTIONS(
                            data.begin(), data.end(), fields.begin(), fields.end());
                    });
            }
        });
        */


    for (size_t i = 0; i != 1050; ++i)
    {
        std::string key = "key" + boost::lexical_cast<std::string>(i);
        rocksDBStorage->asyncGetRow(nullptr, key, [&](Error::Ptr&& error, Entry::Ptr&& entry) {
            BOOST_CHECK_EQUAL(error, nullptr);
            BOOST_CHECK_NE(entry, nullptr);

            auto& data = entry->fields();
            auto fields = std::vector<std::string>({"value_" + boost::lexical_cast<std::string>(i),
                "value1", "value2", "value3", "value4", "value5"});
            BOOST_CHECK_EQUAL_COLLECTIONS(data.begin(), data.end(), fields.begin(), fields.end());
        });
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

BOOST_AUTO_TEST_SUITE_END()

}  // namespace bcos::test