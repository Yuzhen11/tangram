#pragma once

#include <type_traits>
#include <cassert>

#include <vector>
#include <unordered_map>
#include <map>
#include <set>
#include <deque>

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

  void Resize(size_t size) {
    buffer_.resize(size);
  }
  char* GetBegin() { return &buffer_[0]; }
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
SArrayBinStream& operator<<(SArrayBinStream& stream, const std::deque<T>& s) {
    size_t len = s.size();
    stream << len;
    for (auto& elem : s)
        stream << elem;
    return stream;
}

template <typename T>
SArrayBinStream& operator>>(SArrayBinStream& stream, std::deque<T>& s) {
    size_t len;
    stream >> len;
    s.clear();
    for (int i = 0; i < len; i++) {
        T elem;
        stream >> elem;
        s.push_back(std::move(elem));
    }
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

template <typename TupleT, size_t N>
struct TupleIO {
  static void TupleOut(const TupleT& t, SArrayBinStream& stream) {
    TupleIO<TupleT, N-1>::TupleOut(t, stream);
    stream << std::get<N-1>(t);
  }
  static void TupleIn(TupleT& t, SArrayBinStream& stream) {
    TupleIO<TupleT, N-1>::TupleOut(t, stream);
    stream >> std::get<N-1>(t);
  }
};
template <typename TupleT>
struct TupleIO<TupleT, 0> {
  static void TupleOut(const TupleT& t, SArrayBinStream& stream) {
    return;
  }
  static void TupleIn(TupleT& t, SArrayBinStream& stream) {
    return;
  }
};
template <typename... ObjT>
SArrayBinStream& operator<<(SArrayBinStream& stream, const std::tuple<ObjT...>& p) {
  TupleIO<decltype(p), sizeof...(ObjT)>::TupleOut(p, stream);
  return stream;
}
template <typename... ObjT>
SArrayBinStream& operator>>(SArrayBinStream& stream, std::tuple<ObjT...>& p) {
  TupleIO<decltype(p), sizeof...(ObjT)>::TupleIn(p, stream);
  return stream;
}

SArrayBinStream& operator<<(SArrayBinStream& stream, const SArrayBinStream& bin);
SArrayBinStream& operator>>(SArrayBinStream& stream, SArrayBinStream& bin);

}  // namespace xyz
