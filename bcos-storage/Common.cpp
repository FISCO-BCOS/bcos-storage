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
 * @brief common functions
 * @file Common.cpp
 * @author: xingqiangbai
 * @date: 2021-10-11
 */


#include "Common.h"
#include <boost/archive/basic_archive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/serialization/vector.hpp>

using namespace std;

namespace bcos::storage
{
std::string encodeEntry(const Entry& entry) {
    std::string value;
    boost::iostreams::stream<boost::iostreams::back_insert_device<std::string>> outputStream(value);
    boost::archive::binary_oarchive archive(outputStream,
        boost::archive::no_header | boost::archive::no_codecvt | boost::archive::no_tracking);
    // TODO: try to optimize the implement of serialization
    auto fields = entry.fields();
    vector<string> data;
    data.reserve(fields.size());
    for (auto& value : fields)
    {
        std::visit(
            [&](auto const& v) { data.push_back(std::string((const char*)v.data(), v.size())); },
            value);
    }
    archive << data;
    outputStream.flush();

    return value;
}

std::optional<Entry> decodeEntry(TableInfo::ConstPtr tableInfo, const std::string_view& buffer) {
    Entry entry(tableInfo);

    boost::iostreams::stream<boost::iostreams::array_source> inputStream(
        buffer.data(), buffer.size());
    boost::archive::binary_iarchive archive(inputStream,
        boost::archive::no_header | boost::archive::no_codecvt | boost::archive::no_tracking);

    std::vector<std::string> data;
    archive >> data;

    entry.importFields(std::move(data));

    return std::optional<Entry>(entry);
}

}  // namespace bcos::storage
