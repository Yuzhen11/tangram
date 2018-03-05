#pragma once

#include <iostream>

#include "store.hpp"

struct Plan {
  Plan(int _id) : id(_id) {}
  ~Plan() = default;
  int id;

  static Store<Plan> plans;
};

template <typename C1, typename C2, typename O1, typename O2, typename M>
struct ProcPlan : public Plan {
  ProcPlan(int id) : Plan(id) {
    std::cout << "creating: " << std::endl;
    std::cout << "type: " << typeid(C1).name() << std::endl;
    std::cout << "type: " << typeid(C2).name() << std::endl;
    std::cout << "type: " << typeid(O1).name() << std::endl;
    std::cout << "type: " << typeid(O2).name() << std::endl;
    std::cout << "type: " << typeid(M).name() << std::endl;
  }
};
