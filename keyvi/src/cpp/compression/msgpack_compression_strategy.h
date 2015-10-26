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
 * msgpack_compression_strategy.h
 *
 *  Created on: October 26, 2015
 *      Author: David Mark Nemeskey<nemeskey.david@gmail.com>
 */

#ifndef MSGPACK_COMPRESSION_STRATEGY_H_
#define MSGPACK_COMPRESSION_STRATEGY_H_

#include <string>
#include <sstream>
#include <boost/lexical_cast.hpp>

#include <msgpack.hpp>


/**
 * A compression strategy that shaves off msgpack overhead for lists that
 * contain fixed-width strings. The asymptotic gain is 1 / (str_length + 1),
 * so e.g. 4% for 24-byte strings. It gets better as the lists become shorter,
 * but will not exceed 1 / (str_length + 2), reached with single-element lists.
 */
struct FixedLengthStringListCompressionStrategy final {
  FixedLengthStringListCompressionStrategy(const std::string& model) {
    try {
      string_length_ = boost::lexical_cast<size_t>(model);
    } catch (boost::bad_lexical_cast const&) {
      throw (std::invalid_argument("Invalid model: " + model));
    }
  }

  std::string Compress(const char* raw, size_t raw_size) {
    std::ostringstream ss;
    msgpack::unpacked result;
    msgpack::unpack(result, raw, raw_size);
    msgpack::object arr_obj = result.get();

    if (arr_obj.type != msgpack::type::ARRAY) {
      throw std::invalid_argument("Not an msgpack array!");
    } else {
      const msgpack::object_array& arr = arr_obj.via.array;
      for (size_t i = 0; i < arr.size; i++) {
        msgpack::object str_obj = arr.ptr[i];
        if (str_obj.type != msgpack::type::STR)
          throw std::invalid_argument("Array element is not a string!");
        const msgpack::object_str& str = str_obj.via.str;

        if (str.size != string_length_)
          throw std::invalid_argument("Array element string: invalid length");
        ss << std::string(str.ptr, str.size);
      }
    }

    return ss.str();
  }

  inline std::string Decompress(const std::string& compressed) {
    msgpack_buffer_.clear();
    msgpack::packer<msgpack::sbuffer> packer(&msgpack_buffer_);
    packer.pack_array(compressed.size() / string_length_);
    for (size_t i = 0; i < compressed.size(); i += string_length_) {
      packer.pack_str(string_length_);
      packer.pack_str_body(&compressed[i], string_length_);
    }
    return std::string(msgpack_buffer_.data(), msgpack_buffer_.size());
  }

  std::string name() const { return "fixed_length_string_list"; }

 private:
  size_t string_length_;
  msgpack::sbuffer msgpack_buffer_;
};

#endif  // MSGPACK_COMPRESSION_STRATEGY_H_
