#pragma once

#include "store.hpp"

struct Collection {
  Collection(int _id) : id(_id) {}
  ~Collection() = default;
  int id;

  static Store<Collection> collections;
};

template <typename O>
struct ProcCollection : public Collection {
  ProcCollection(int id) : Collection(id) {}
  using D = O;
};
