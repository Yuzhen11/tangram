#pragma once

#include "store.hpp"
#include <iostream>

struct Dataflow {
  Dataflow(int _id) : id(_id) {}
  ~Dataflow() = default;
  int id;

  virtual void visit() = 0;
  static Store<Dataflow> dataflows;
};

template <typename O> struct ProcDataflow : virtual public Dataflow {
  ProcDataflow(int id) : Dataflow(id) {}
};

template <typename O> struct DataflowDependency : virtual public Dataflow {
  DataflowDependency(int id) : Dataflow(id) {}

  std::vector<ProcDataflow<O> *> children;
};

template <typename D> struct Distribute : public DataflowDependency<D> {
  Distribute(int id) : DataflowDependency<D>(id), Dataflow(id) {}

  virtual void visit() override {
    std::cout << "visit: " << this->id << " distribute" << std::endl;
    std::cout << "children size: " << this->children.size() << std::endl;
    for (auto *child : this->children) {
      child->visit();
    }
  }
};

template <typename I, typename M, typename O>
struct Map : public ProcDataflow<I>, public DataflowDependency<O> {
  Map(int id, M mapper)
      : ProcDataflow<I>(id), DataflowDependency<O>(id), Dataflow(id) {}

  virtual void visit() override {
    std::cout << "visit: " << this->id << " map" << std::endl;
    std::cout << "children size: " << DataflowDependency<O>::children.size()
              << std::endl;
    for (auto *child : DataflowDependency<O>::children) {
      child->visit();
    }
  }
};

template <typename I1, typename I2, typename J>
struct Join : public ProcDataflow<I1>, public ProcDataflow<I2>, public DataflowDependency<I1> {
  Join(int id, J join)
      : DataflowDependency<I1>(id), ProcDataflow<I1>(id), ProcDataflow<I2>(id), Dataflow(id) {}
  virtual void visit() override {
    std::cout << "visit: " << this->id << " join" << std::endl;
    std::cout << "children size: " << DataflowDependency<I1>::children.size()
              << std::endl;
  }
};
