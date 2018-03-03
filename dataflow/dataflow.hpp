#pragma once

#include "store.hpp"
#include <iostream>

namespace nova {

struct Dataflow {
  Dataflow(int _id) : id(_id) {}
  int id;

  virtual void visit() = 0;
  static Store<Dataflow> dataflows;
};

template <typename O> struct ProcDataflow;

template <typename O> struct DataflowDependency {
  std::vector<ProcDataflow<O> *> children;
};

template <typename O> struct ProcDataflow : virtual public Dataflow {
  ProcDataflow(int id) : Dataflow(id) {}

  DataflowDependency<O> dep;
};

template <typename D> struct Distribute : public ProcDataflow<D> {
  Distribute(int id) : ProcDataflow<D>(id), Dataflow(id) {}

  virtual void visit() override {
    std::cout << "visit: " << this->id << " distribute" << std::endl;
    std::cout << "children size: " << this->dep.children.size() << std::endl;
    for (auto *child : this->dep.children) {
      child->visit();
    }
  }
};

template <typename I, typename M, typename O>
struct Map : public ProcDataflow<I>, public ProcDataflow<O> {
  Map(int id, M mapper)
      : ProcDataflow<I>(id), ProcDataflow<O>(id), Dataflow(id) {}

  virtual void visit() override {
    std::cout << "visit: " << this->id << " map" << std::endl;
    std::cout << "children size: " << ProcDataflow<O>::dep.children.size()
              << std::endl;
    for (auto *child : ProcDataflow<O>::dep.children) {
      child->visit();
    }
  }
};

template <typename I1, typename I2, typename J>
struct Join : public ProcDataflow<I1>, public ProcDataflow<I2> {
  Join(int id, J join)
      : ProcDataflow<I1>(id), ProcDataflow<I2>(id), Dataflow(id) {}
  virtual void visit() override {
    std::cout << "visit: " << this->id << " join" << std::endl;
    std::cout << "children size: " << ProcDataflow<I2>::dep.children.size()
              << std::endl;
  }
};

} // namespace nova
