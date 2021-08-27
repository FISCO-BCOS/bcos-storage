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
#include "RocksDBStorage.h"
#include "bcos-framework/libutilities/Error.h"
#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/iostreams/stream.hpp"
#include <rocksdb/options.h>
#include <exception>

using namespace bcos::storage;
using namespace rocksdb;

const char* const TABLE_KEY_SPLIT = ":";

void RocksDBStorage::asyncGetPrimaryKeys(const TableInfo::Ptr& _tableInfo,
    const Condition::Ptr& _condition,
    std::function<void(Error::Ptr&&, std::vector<std::string>&&)> _callback) noexcept
{
    std::vector<std::string> result;

    auto keyPrefix = _tableInfo->name + TABLE_KEY_SPLIT;

    ReadOptions read_options;
    read_options.auto_prefix_mode = true;
    auto iter = m_db->NewIterator(read_options);
    for (iter->Seek(keyPrefix); iter->Valid() && iter->key().starts_with(keyPrefix); iter->Next())
    {
        size_t start = keyPrefix.size();
        if (!_condition || _condition->isValid(std::string_view(
                               iter->key().data() + start, iter->key().size() - start)))
        {  // filter by condition, the key need
           // remove TABLE_PREFIX
            result.emplace_back(iter->key().ToString().substr(start));
        }
    }
    delete iter;

    _callback(nullptr, std::move(result));
}

void RocksDBStorage::asyncGetRow(const TableInfo::Ptr& _tableInfo, const std::string& _key,
    std::function<void(Error::Ptr&&, Entry::Ptr&&)> _callback) noexcept
{
    try
    {
        std::string value;

        auto dbKey = toDBKey(_tableInfo->name, _key);
        auto status = m_db->Get(ReadOptions(), Slice(dbKey.data(), dbKey.size()), &value);

        if (!status.ok())
        {
            if (status.IsNotFound())
            {
                _callback(nullptr, nullptr);
                return;
            }

            std::string errorMessage = "RocksDB get failed!, " +
                                       boost::lexical_cast<std::string>(status.subcode()) + " " +
                                       status.getState();
            _callback(BCOS_ERROR_PTR(-1, errorMessage), nullptr);

            return;
        }

        auto entry = std::make_shared<Entry>(_tableInfo, 0);

        boost::iostreams::stream<boost::iostreams::array_source> inputStream(
            value.c_str(), value.size());
        boost::archive::binary_iarchive archive(inputStream);

        std::vector<std::string> fields;
        archive >> fields;

        entry->importData(std::move(fields));

        _callback(nullptr, std::move(entry));
    }
    catch (std::exception e)
    {
        // TODO: _callback(BCOS_ERROR_WITH_PREV_PTR(-1, "Get row failed!", e), nullptr);
        _callback(
            std::make_shared<bcos::Error>(BCOS_ERROR_WITH_PREV(-1, "Get row failed!", e)), nullptr);
    }
}

void RocksDBStorage::asyncGetRows(const TableInfo::Ptr& _tableInfo,
    const gsl::span<std::string>& _keys,
    std::function<void(Error::Ptr&&, std::vector<Entry::Ptr>&&)> _callback) noexcept
{
    try
    {
        std::vector<std::string> dbKeys(_keys.size());
        std::vector<Slice> slices(_keys.size());
        tbb::parallel_for(tbb::blocked_range<size_t>(0, _keys.size()),
            [&](const tbb::blocked_range<size_t>& range) {
                for (size_t i = range.begin(); i != range.end(); ++i)
                {
                    dbKeys[i] = toDBKey(_tableInfo->name, _keys[i]);
                    slices[i] = Slice(dbKeys[i].data(), dbKeys[i].size());
                }
            });

        std::vector<std::string> values;
        auto statusList = m_db->MultiGet(ReadOptions(), slices, &values);

        std::vector<Entry::Ptr> entries(_keys.size());
        tbb::parallel_for(tbb::blocked_range<size_t>(0, _keys.size()),
            [&](const tbb::blocked_range<size_t>& range) {
                for (size_t i = range.begin(); i != range.end(); ++i)
                {
                    auto& status = statusList[i];
                    auto& value = values[i];

                    if (status.ok())
                    {
                        auto entry = std::make_shared<Entry>(_tableInfo, 0);

                        boost::iostreams::stream<boost::iostreams::array_source> inputStream(value);
                        boost::archive::binary_iarchive archive(inputStream);

                        std::vector<std::string> fields;
                        archive >> fields;

                        entry->importData(std::move(fields));

                        entries[i] = std::move(entry);
                    }
                    else
                    {
                        entries[i] = nullptr;
                    }
                }
            });

        _callback(nullptr, std::move(entries));
    }
    catch (const std::exception& e)
    {
        _callback(std::make_shared<bcos::Error>(BCOS_ERROR_WITH_PREV(-1, "Get rows failed! ", e)),
            std::vector<Entry::Ptr>());
    }
}

void RocksDBStorage::asyncSetRow(const TableInfo::Ptr& tableInfo, const std::string& key,
    const Entry::Ptr& entry, std::function<void(Error::Ptr&&, bool)> callback) noexcept
{
    try
    {
        auto dbKey = toDBKey(tableInfo->name, key);

        std::string value;
        boost::iostreams::stream<boost::iostreams::array_sink> outputStream(value);
        boost::archive::binary_oarchive archive(outputStream);

        Entry encodeEntry(*entry);  // TODO: change to ro interface
        auto data = encodeEntry.exportData();
        archive << data;

        WriteOptions options;
        auto status = m_db->Put(WriteOptions(), dbKey, value);
        if (!status.ok())
        {
            callback(BCOS_ERROR_PTR(-1, std::string("Set row failed! ") +
                                            boost::lexical_cast<std::string>(status.subcode()) +
                                            " " + status.getState()),
                false);
            return;
        }

        callback(nullptr, true);
    }
    catch (const std::exception& e)
    {
        callback(
            std::make_shared<bcos::Error>(BCOS_ERROR_WITH_PREV(-1, "Set row failed! ", e)), false);
    }
}

void RocksDBStorage::asyncPrepare(const PrepareParams&,
    const TraverseStorageInterface::Ptr& storage,
    std::function<void(Error::Ptr&&)> callback) noexcept
{
    try
    {
        rocksdb::WriteBatch writeBatch;
        storage->parallelTraverse(true, [&](const TableInfo::Ptr& tableInfo, const std::string& key,
                                            const Entry::ConstPtr& entry) {
            auto dbKey = toDBKey(tableInfo->name, key);
            if (entry->status() == Entry::DELETED)
            {
                writeBatch.Delete(dbKey);
            }
            else
            {
                std::string value;
                boost::iostreams::stream<boost::iostreams::array_sink> outputStream(value);
                boost::archive::binary_oarchive archive(outputStream);

                Entry encodeEntry(*entry);  // TODO: change to ro interface
                auto data = encodeEntry.exportData();
                archive << data;

                auto status = writeBatch.Put(dbKey, value);
            }
            return true;
        });

        m_db->Write(WriteOptions(), &writeBatch);

        callback(nullptr);
    }
    catch (const std::exception& e)
    {
        callback(std::make_shared<bcos::Error>(BCOS_ERROR_WITH_PREV(-1, "Prepare failed! ", e)));
    }
}

void RocksDBStorage::aysncCommit(
    protocol::BlockNumber, std::function<void(Error::Ptr&&)> callback) noexcept
{
    callback(nullptr);
}

void RocksDBStorage::aysncRollback(
    protocol::BlockNumber, std::function<void(Error::Ptr&&)> callback) noexcept
{
    callback(nullptr);
}

std::string RocksDBStorage::toDBKey(const std::string_view& table, const std::string_view& key)
{
    std::string dbKey;
    dbKey.append(table).append(TABLE_KEY_SPLIT).append(key);

    return dbKey;
}