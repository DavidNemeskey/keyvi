/* * keyvi - A key value store.
 *
 * Copyright 2015 Hendrik Muhs<hendrik.muhs@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * json_value_store.h
 *
 *  Created on: Jul 16, 2014
 *      Author: hendrik
 */

#ifndef JSON_VALUE_STORE_H_
#define JSON_VALUE_STORE_H_

#include <boost/functional/hash.hpp>
#include <zlib.h>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

#include "dictionary/fsa/internal/ivalue_store.h"
#include "dictionary/fsa/internal/serialization_utils.h"
#include "dictionary/fsa/internal/lru_generation_cache.h"
#include "dictionary/fsa/internal/memory_map_manager.h"
#include "dictionary/util/vint.h"

#include "dictionary/util/json_to_msgpack_fwd.h"
#include "msgpack.hpp"
#include "dictionary/util/json_to_msgpack.h"
#include "msgpack/zbuffer.hpp"

#include "compression/predictive_compression.h"

//#define ENABLE_TRACING
#include "dictionary/util/trace.h"

namespace keyvi {
namespace dictionary {
namespace fsa {
namespace internal {

/**
 * Value store where the value consists of a single integer.
 */
class JsonValueStore
final {
   public:

    struct RawPointer
    final {
       public:
        RawPointer()
            : RawPointer(0, 0, 0) {
        }

        RawPointer(uint64_t offset, int hashcode, size_t length)
            : offset_(offset),
              hashcode_(hashcode),
              length_(length) {

          if (length > USHRT_MAX) {
            length_ = USHRT_MAX;
          }

        }

        int GetHashcode() const {
          return hashcode_;
        }

        uint64_t GetOffset() const {
          return offset_;
        }

        ushort GetLength() const {
          return length_;
        }

        int GetCookie() const {
          return cookie_;
        }

        void SetCookie(int value) {
          cookie_ = static_cast<ushort>(value);
        }

        bool IsEmpty() const {
          return offset_ == 0 && hashcode_ == 0 && length_ == 0;
        }

        bool operator==(const RawPointer& l) {
          return offset_ == l.offset_;
        }

        static size_t GetMaxCookieSize(){
          return MaxCookieSize;
        }

       private:
        static const size_t MaxCookieSize = 0xFFFF;

        uint64_t offset_;
        int32_t hashcode_;
        ushort length_;
        ushort cookie_ = 0;

      };

      template<class PersistenceT>
      struct RawPointerForCompare
      final
      {
         public:
          RawPointerForCompare(const std::string& value,
                               PersistenceT* persistence)
              : value_(value),
                persistence_(persistence) {
            hashcode_ = std::hash<value_t>()(value);
            length_ = value.size();
          }

          int GetHashcode() const {
            return hashcode_;
          }

          bool operator==(const RawPointer& l) const {
            // First filter - check if hash code  is the same
            if (l.GetHashcode() != hashcode_) {
              return false;
            }
            TRACE("check");
            size_t length_l = l.GetLength();

            if (length_l < USHRT_MAX && length_l != length_) {
              return false;
            }

            TRACE("Compare values at data level length: %d %d", l.GetOffset(), value_.size());

            return persistence_->Compare(l.GetOffset(), (void*) value_.data(), value_.size());
          }

         private:
          std::string value_;
          PersistenceT* persistence_;
          int32_t hashcode_;
          size_t length_;
        };

        typedef std::string value_t;
        static const uint64_t no_value = 0;
        static const bool inner_weight = false;

        JsonValueStore(boost::filesystem::path temporary_path, size_t memory_limit = 104857600): hash_(memory_limit){
          temporary_directory_ = temporary_path;
          temporary_directory_ /= boost::filesystem::unique_path(
              "dictionary-fsa-json_value_store-%%%%-%%%%-%%%%-%%%%");
          boost::filesystem::create_directory(temporary_directory_);

          size_t external_memory_chunk_size = 1073741824;

          values_extern_ = new MemoryMapManager(external_memory_chunk_size,
                                                            temporary_directory_,
                                                           "json_values_filebuffer");
          compressor_ = new compression::PredictiveCompression("/run/shm/norm.c");
        }

        ~JsonValueStore(){
          delete compressor_;
          delete values_extern_;
          boost::filesystem::remove_all(temporary_directory_);
        }

        JsonValueStore() = delete;
        JsonValueStore& operator=(JsonValueStore const&) = delete;
        JsonValueStore(const JsonValueStore& that) = delete;

        /**
         * Simple implementation of a value store for json values:
         * todo: performance improvements?
         */
        uint64_t GetValue(const value_t& value, bool& no_minimization) {
          std::string packed_value;
          msgpack_buffer_.clear();

          ++number_of_values_;

          rapidjson::Document json_document;
          json_document.Parse(value.c_str());

          if (!json_document.HasParseError()){
            TRACE("Got json");
            msgpack::pack(&msgpack_buffer_, json_document);
          } else {
            TRACE("Got a normal string");
            msgpack::pack(&msgpack_buffer_, value);
          }

          // zlib compression
          if (msgpack_buffer_.size() > 32) {
            packed_value = compress_string(msgpack_buffer_.data(), msgpack_buffer_.size());
          } else {
            util::encodeVarint(msgpack_buffer_.size(), packed_value);
            packed_value.append(msgpack_buffer_.data(), msgpack_buffer_.size());
          }

          TRACE("Packed value: %s", packed_value.c_str());

          const RawPointerForCompare<MemoryMapManager> stp(packed_value,
                                                            values_extern_);
          const RawPointer p = hash_.Get(stp);

          if (!p.IsEmpty()) {
            // found the same value again, minimize
            TRACE("Minimized value");
            return p.GetOffset();
          } // else persist string value

          no_minimization = true;
          TRACE("New unique value");
          ++number_of_unique_values_;

          uint64_t pt = static_cast<uint64_t>(values_buffer_size_);

          values_extern_->Append(values_buffer_size_, (void*)packed_value.data(), packed_value.size());
          values_buffer_size_ += packed_value.size();

          TRACE("add value to hash at %d, length %d", pt, packed_value.size());
          hash_.Add(RawPointer(pt, stp.GetHashcode(), packed_value.size()));

          return pt;
        }

        uint32_t GetWeightValue(value_t value) const {
          return 0;
        }

        value_store_t GetValueStoreType() const {
          return JSON_VALUE_STORE;
        }

        void Write(std::ostream& stream) {

          boost::property_tree::ptree pt;
          pt.put("size", std::to_string(values_buffer_size_));
          pt.put("values", std::to_string(number_of_values_));
          pt.put("unique_values", std::to_string(number_of_unique_values_));

          internal::SerializationUtils::WriteJsonRecord(stream, pt);
          TRACE("Wrote JSON header, stream at %d", stream.tellp());

          values_extern_->Write(stream, values_buffer_size_);
        }

       private:
        MemoryMapManager* values_extern_;

        compression::PredictiveCompression* compressor_;

        LeastRecentlyUsedGenerationsCache<RawPointer> hash_;
        msgpack::sbuffer msgpack_buffer_;
        char zlib_buffer_[32768];
        size_t number_of_values_ = 0;
        size_t number_of_unique_values_ = 0;
        size_t values_buffer_size_ = 0;
        boost::filesystem::path temporary_directory_;

        /** Compress a STL string using zlib with given compression level and return
          * the binary data. */
        std::string compress_string(const char* data, size_t data_size,
                                    int compressionlevel = Z_BEST_COMPRESSION)
        {
            std::string outstring = " ";
            std::string to_compress(data, data_size);
            outstring += compressor_->Compress(to_compress);

            std::string size_prefix;
            util::encodeVarint(outstring.size(), size_prefix);

            return size_prefix + outstring;
        }
      };

      class JsonValueStoreReader final: public IValueStoreReader {
       public:
        using IValueStoreReader::IValueStoreReader;

        JsonValueStoreReader(std::istream& stream,
                               boost::interprocess::file_mapping* file_mapping)
            : IValueStoreReader(stream, file_mapping) {

          TRACE("JsonValueStoreReader construct");

          properties_ =
              internal::SerializationUtils::ReadJsonRecord(stream);

          size_t offset = stream.tellg();
          size_t strings_size = properties_.get<size_t>("size");

          // check for file truncation
          if (strings_size > 0) {
            stream.seekg(strings_size - 1, stream.cur);
            if (stream.peek() == EOF) {
              throw std::invalid_argument("file is corrupt(truncated)");
            }
          }

          strings_region_ = new boost::interprocess::mapped_region(
              *file_mapping, boost::interprocess::read_only, offset,
              strings_size);

          strings_ = (const char*) strings_region_->get_address();

          decompressor_ = new compression::PredictiveCompression("/run/shm/norm.c");
        }

        ~JsonValueStoreReader() {
          delete decompressor_;
          delete strings_region_;
        }

        virtual attributes_t GetValueAsAttributeVector(uint64_t fsa_value)
            override {
          attributes_t attributes(new attributes_raw_t());

          std::string raw_value = util::decodeVarintString(strings_ + fsa_value);

          //auto length = util::decodeVarint((uint8_t*) strings_ + fsa_value);
          //std::string raw_value(strings_ + fsa_value, length);

          (*attributes)["value"] = raw_value;
          return attributes;
        }

        virtual std::string GetRawValueAsString(uint64_t fsa_value) override {
          return util::decodeVarintString(strings_ + fsa_value);
        }

        virtual std::string GetValueAsString(uint64_t fsa_value) override {
          TRACE("JsonValueStoreReader GetValueAsString");
          std::string packed_string = util::decodeVarintString(strings_ + fsa_value);

          if (packed_string[0] == ' '){
            TRACE("unpack zlib compressed string");
            packed_string = decompress_string(packed_string);

            TRACE("unpacking %s", packed_string.c_str());
          }

          msgpack::unpacked doc;
          msgpack::unpack(&doc, packed_string.data(), packed_string.size());
          rapidjson::Document json_document;

          doc.get().convert(&json_document);

          rapidjson::StringBuffer buffer;
          rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
          json_document.Accept(writer);
          return buffer.GetString();
        }

        virtual std::string GetStatistics() const {
          std::ostringstream buf;
          boost::property_tree::write_json (buf, properties_, false);
          return buf.str();
        }

       private:
        compression::PredictiveCompression* decompressor_;

        boost::interprocess::mapped_region* strings_region_;
        const char* strings_;
        boost::property_tree::ptree properties_;

        /** Decompress an STL string using zlib and return the original data. */
        std::string decompress_string(const std::string& str)
        {
          return decompressor_->Uncompress(str.substr(1));
        }
      };

} /* namespace internal */
} /* namespace fsa */
} /* namespace dictionary */
} /* namespace keyvi */

#endif /* JSON_VALUE_STORE_H_ */
