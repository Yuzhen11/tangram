#pragma once

#include <vector>

template <typename Base> class Store {
public:
  template <typename O = Base, typename... ArgT> O *make(ArgT... args) {
    auto id = instances.size();
    O *oper = new O(id, args...);
    instances.push_back(oper);
    return oper;
  }

  /*
  template<typename O = Base, typename ... ArgT>
  O &make_move(ArgT &&... args) {
    auto id = instances.size();
    O *oper = new O(id, std::move(args ...));
    instances.push_back(oper);
    return *oper;
  }

  template<typename O = Base, typename ... ArgT>
  O &make_ref(ArgT &... args) {
    auto id = instances.size();
    O *oper = new O(id, args ...);
    instances.push_back(oper);
    return *oper;
  }
  */

  Base *get(unsigned id) { return instances.at(id); }

  const auto &all() { return instances; }

private:
  // maybe not necessary to delete the pointers.
  std::vector<Base *> instances;
};

