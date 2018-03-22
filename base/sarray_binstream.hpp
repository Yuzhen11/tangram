#pragma once

#include <type_traits>
#include <cassert>

#include <vector>
#include <unordered_map>
#include <map>
#include <set>

#include "base/third_party/sarray.h"
#include "base/message.hpp"

#include "glog/logging.h"

namespace xyz {

class SArrayBinStream {
 public:
  SArrayBinStream() = default;
  ~SArrayBinStream() = default;

  size_t Size() const;

  void AddBin(const char* bin, size_t sz);

  void* PopBin(size_t sz);

  Message ToMsg() const;
  void FromMsg(const Message& msg);

  /*
   * From and to SArray.
   */
  template <typename V>
  void FromSArray(const third_party::SArray<V>& sarray) {
    buffer_ = sarray;
    front_ = 0;
  }
  third_party::SArray<char> ToSArray() const;

  inline const char* GetPtr() const { return (&buffer_[0]) + front_; }
  void CopyFrom(const char* data, size_t size) {
    buffer_.CopyFrom(data, size);
    front_ = 0;
  }
 private:
  third_party::SArray<char> buffer_;
  size_t front_ = 0;
};

/*
 * Trivially copyable type.
 */
template <typename T>
SArrayBinStream& operator<<(SArrayBinStream& bin, const T& t) {
  static_assert(std::is_trivially_copyable<T>::value, 
        "For non trivially copyable type, serialization functions are needed");
  bin.AddBin((char*)&t, sizeof(T));
  return bin;
}

template <typename T>
SArrayBinStream& operator>>(SArrayBinStream& bin, T& t) {
  static_assert(std::is_trivially_copyable<T>::value, 
        "For non trivially copyable type, serialization functions are needed");
  t = *(T*)(bin.PopBin(sizeof(T)));
  return bin;
}

/*
 * string type.
 */
template <typename InputT>
SArrayBinStream& operator<<(SArrayBinStream& stream, const std::basic_string<InputT>& v) {
    size_t len = v.size();
    stream << len;
    for (auto& elem : v)
        stream << elem;
    return stream;
}

template <typename OutputT>
SArrayBinStream& operator>>(SArrayBinStream& stream, std::basic_string<OutputT>& v) {
    size_t len;
    stream >> len;
    v.clear();
    try {
        v.resize(len);
    } catch (std::exception e) {
        assert(false);
    }
    for (auto& elem : v)
        stream >> elem;
    return stream;
}

template <typename InputT>
SArrayBinStream& operator<<(SArrayBinStream& stream, const std::vector<InputT>& v) {
    size_t len = v.size();
    stream << len;
    for (int i = 0; i < v.size(); ++i)
        stream << v[i];
    return stream;
}

template <typename OutputT>
SArrayBinStream& operator>>(SArrayBinStream& stream, std::vector<OutputT>& v) {
    size_t len;
    stream >> len;
    v.clear();
    v.resize(len);
    for (int i = 0; i < v.size(); ++i)
        stream >> v[i];
    return stream;
}

template <typename T>
SArrayBinStream& operator<<(SArrayBinStream& stream, const std::set<T>& s) {
    size_t len = s.size();
    stream << len;
    for (auto& elem : s)
        stream << elem;
    return stream;
}

template <typename T>
SArrayBinStream& operator>>(SArrayBinStream& stream, std::set<T>& s) {
    size_t len;
    stream >> len;
    s.clear();
    for (int i = 0; i < len; i++) {
        T elem;
        stream >> elem;
        s.insert(std::move(elem));
    }
    return stream;
}

template <typename K, typename V>
SArrayBinStream& operator<<(SArrayBinStream& stream, const std::map<K, V>& map) {
    size_t len = map.size();
    stream << len;
    for (auto& elem : map)
        stream << elem;
    return stream;
}

template <typename K, typename V>
SArrayBinStream& operator>>(SArrayBinStream& stream, std::map<K, V>& map) {
    size_t len;
    stream >> len;
    map.clear();
    for (int i = 0; i < len; i++) {
        std::pair<K, V> elem;
        stream >> elem;
        map.insert(elem);
    }
    return stream;
}

template <typename K, typename V>
SArrayBinStream& operator<<(SArrayBinStream& stream, const std::unordered_map<K, V>& unordered_map) {
    size_t len = unordered_map.size();
    stream << len;
    for (auto& elem : unordered_map)
        stream << elem;
    return stream;
}

template <typename K, typename V>
SArrayBinStream& operator>>(SArrayBinStream& stream, std::unordered_map<K, V>& unordered_map) {
    size_t len;
    stream >> len;
    unordered_map.clear();
    for (int i = 0; i < len; i++) {
        std::pair<K, V> elem;
        stream >> elem;
        unordered_map.insert(elem);
    }
    return stream;
}

template <typename FirstT, typename SecondT>
SArrayBinStream& operator<<(SArrayBinStream& stream, const std::pair<FirstT, SecondT>& p) {
    stream << p.first << p.second;
    return stream;
}

template <typename FirstT, typename SecondT>
SArrayBinStream& operator>>(SArrayBinStream& stream, std::pair<FirstT, SecondT>& p) {
    stream >> p.first >> p.second;
    return stream;
}

template <typename FirstT, typename SecondT, typename ThirdT>
SArrayBinStream& operator<<(SArrayBinStream& stream, const std::tuple<FirstT, SecondT, ThirdT>& p) {
    stream << std::get<0>(p) << std::get<1>(p) << std::get<2>(p);
    return stream;
}

template <typename FirstT, typename SecondT, typename ThirdT>
SArrayBinStream& operator>>(SArrayBinStream& stream, std::tuple<FirstT, SecondT, ThirdT>& p) {
    stream >> std::get<0>(p) >> std::get<1>(p) >> std::get<2>(p);
    return stream;
}

SArrayBinStream& operator<<(SArrayBinStream& stream, const SArrayBinStream& bin);
SArrayBinStream& operator>>(SArrayBinStream& stream, SArrayBinStream& bin);

}  // namespace xyz
