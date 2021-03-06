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
 * dictionary_compiler.h
 *
 *  Created on: Jul 17, 2014
 *      Author: hendrik
 */

#ifndef DICTIONARY_COMPILER_H_
#define DICTIONARY_COMPILER_H_

#include <algorithm>
#include <functional>
#include "tpie/serialization_sorter.h"

#include "dictionary/util/tpie_initializer.h"
#include "dictionary/fsa/internal/null_value_store.h"
#include "dictionary/fsa/generator.h"

//#define ENABLE_TRACING
#include "dictionary/util/trace.h"

namespace keyvi {
namespace dictionary {

/**
 * structure for internal processing
 * Note: Not using std::pair because it did not compile with Tpie
 */
template<typename value_t>
struct key_value_pair {
  key_value_pair() : key(), value() {
  }

  key_value_pair(const std::string& k, const value_t v): key(k), value(v) {}

  bool operator<(const key_value_pair kv) const {
    return key < kv.key;
  }

  std::string key;
  value_t value;
};

/**
 * Tpie serialization and deserialization for sorting.
 */
template<typename Dst, typename v>
void serialize(Dst & d, const keyvi::dictionary::key_value_pair<v> & pt) {
  using tpie::serialize;
  serialize(d, pt.key);
  serialize(d, pt.value);
}
template<typename Src, typename v>
void unserialize(Src & s, keyvi::dictionary::key_value_pair<v> & pt) {
  using tpie::unserialize;
  unserialize(s, pt.key);
  unserialize(s, pt.value);
}

/**
 * Dictionary Compiler
 */
template<class PersistenceT, class ValueStoreT = fsa::internal::NullValueStore>
class DictionaryCompiler
  final {
    typedef const fsa::internal::IValueStoreWriter::vs_param_t vs_param_t;
    typedef key_value_pair<typename ValueStoreT::value_t> key_value_t;
    typedef std::function<void (size_t , size_t, void*)> callback_t;

   public:
    /**
     * Instantiate a dictionary compiler.
     *
     * Note the memory limit only limits the memory used for internal buffers,
     * memory usage for small short-lived objects and the library itself is part of the limit.
     *
     * @param memory_limit memory limit for internal memory usage
     */
    DictionaryCompiler(size_t memory_limit = 1073741824,
                       const vs_param_t& value_store_params = vs_param_t())
        : initializer_(util::TpieIntializer::getInstance()),
          sorter_(),
          generator_(memory_limit, value_store_params) {
      sorter_.set_available_memory(memory_limit);
      sorter_.begin();
    }

    template<typename StringType>
    void Add(StringType input_key, typename ValueStoreT::value_t value =
                 ValueStoreT::no_value) {
      sorter_.push(key_value_t(input_key, value));
    }

#ifdef Py_PYTHON_H
    template<typename StringType>
    void __setitem__ (StringType input_key, typename ValueStoreT::value_t value =
        ValueStoreT::no_value) {
      return Add(input_key, value);
    }
#endif

    /**
     * Do the final compilation
     * todo: implement some sort of progress callback
     */
    void Compile(callback_t progress_callback = nullptr, void* user_data = nullptr) {
      sorter_.end();
      sorter_.merge_runs();
      {
        number_of_items_ = sorter_.item_count();

        callback_trigger_ = 1+(number_of_items_-1)/100;

        if (callback_trigger_ > 100000) {
          callback_trigger_ = 100000;
        }

        while (sorter_.can_pull()) {
          key_value_t key_value = sorter_.pull();

          TRACE("adding to generator: %s", key_value.key.c_str());

          generator_.Add(key_value.key, key_value.value);
          ++added_key_values_;
          if (progress_callback && (added_key_values_ % callback_trigger_ == 0)){
            progress_callback(added_key_values_, number_of_items_, user_data);
          }
        }
      }

      generator_.CloseFeeding();
    }

    /**
     * Compile a partition
     *
     * @param maximum_partition_size
     * @param stream
     * @param maximum_partition_depth
     * @return
     */
    bool CompileNext(size_t maximum_partition_size, std::ostream& stream, int maximum_partition_depth = 2,
                     callback_t progress_callback = nullptr, void* user_data = nullptr)
    {
      if (!sort_finalized_){
        sorter_.end();
        sorter_.merge_runs();
        sort_finalized_ = true;

        number_of_items_ = sorter_.item_count();
        callback_trigger_ = 1+(number_of_items_-1)/100;
        if (callback_trigger_ > 100000) {
          callback_trigger_ = 100000;
        }
      }

      size_t partition_conservative_size = (maximum_partition_size / 10) * 9;

      TRACE("Compile next partition");
      {
        int i = 0;

        //size_t number_of_items = sorter_.item_count();

        while (sorter_.can_pull()) {
          key_value_t key_value = sorter_.pull();

          generator_.Add(key_value.key, key_value.value);
          ++i;
          ++added_key_values_;

          if (progress_callback && (added_key_values_ % callback_trigger_ == 0)){
            progress_callback(added_key_values_, number_of_items_, user_data);
          }

          if (i % 1000 == 0 && generator_.GetFsaSize() > partition_conservative_size){
            // todo: continue feeding until we found a good point to persist the partition

            TRACE("finish partition: find good partition end");
            std::string last_key (key_value.key);
            bool make_new_partition = false;

            while (sorter_.can_pull()) {
              key_value = sorter_.pull();

              if (fsa::get_common_prefix_length(last_key.c_str(), key_value.key.c_str()) < maximum_partition_depth) {
                make_new_partition = true;
                break;
              }

              generator_.Add(key_value.key, key_value.value);
              ++added_key_values_;

              if (progress_callback && (added_key_values_ % callback_trigger_ == 0)){
                progress_callback(added_key_values_, number_of_items_, user_data);
              }

              last_key = key_value.key;
            }
            TRACE("finish partition: finalize partition");

            generator_.CloseFeeding();
            generator_.Write(stream);

            // handle case where we do not have to start a new partition
            if (!make_new_partition){
              return false;
            }

            generator_.Reset();
            generator_.Add(key_value.key, key_value.value);

            return true;
          }
        }
      }

      // finalize the last partition
      generator_.CloseFeeding();
      generator_.Write(stream);

      return false;
    }

    /**
     * Set a custom manifest to be embedded into the index file.
     *
     * @param manifest as JSON string
     */
    template<typename StringType>
    void SetManifestFromString(StringType manifest){
      generator_.SetManifestFromString(manifest);
    }

    /**
     * Set a custom manifest to be embedded into the index file.
     *
     * @param manifest
     */
    void SetManifest(const boost::property_tree::ptree& manifest){
      generator_.SetManifest(manifest);
    }

    void Write(std::ostream& stream) {
       generator_.Write(stream);
    }

    template<typename StringType>
    void WriteToFile(StringType filename) {
      std::ofstream out_stream(filename, std::ios::binary);
      generator_.Write(out_stream);
      out_stream.close();
    }

   private:
    util::TpieIntializer& initializer_;
    tpie::serialization_sorter<key_value_t> sorter_;
    fsa::Generator<PersistenceT, ValueStoreT> generator_;
    bool sort_finalized_ = false;
    size_t added_key_values_ = 0;
    size_t number_of_items_ = 0;
    size_t callback_trigger_ = 0;
  };

} /* namespace dictionary */
} /* namespace keyvi */

#endif /* DICTIONARY_COMPILER_H_ */
