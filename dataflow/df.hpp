#include "dataflow.hpp"
#include "collection.hpp"
#include "plan.hpp"

#include <functional>

template <typename I, typename C> 
struct DF {
  DF() {} // TODO
  DF(DataflowDependency<I> *op, C* c) : op_(op), c_(c) {}

  using II = I;
  using CC = C;

  template <typename O, typename M> DF<O, C> map(M mapper) {
    auto *p = Dataflow::dataflows.make<Map<I, M, O>>(mapper);
    op_->children.push_back(p);
    DF<O, C> df(p, c_);
    if (f_ == nullptr) {
      df.f_ = [mapper](typename C::D d)->O {
        return mapper(d);
      };
    } else {
      df.f_ = [this, mapper](typename C::D d)->O {
        return mapper(f_(d));
      };
    }
    return df;
  }

  template <typename M> auto map(M mapper) {
    return map<decltype(mapper(*(I *)nullptr))>(mapper);
  }

  template <typename D, typename J> auto join(D df, J join) {
    using MII = typename D::II::second_type;
    decltype(join((I *)nullptr, *(MII *)nullptr));
    static_assert(
        std::is_same<decltype(join((I *)nullptr, *(MII *)nullptr)), void>::value,
        "err");
    std::cout << "Msg type: " << typeid(MII).name() << std::endl;
    std::cout << "Obj type: " << typeid(I).name() << std::endl;

    auto *p = Dataflow::dataflows.make<Join<I, typename D::II, J>>(join);
    op_->children.push_back(p);
    df.op_->children.push_back(p);


    Plan::plans.make<ProcPlan<C, typename D::CC, I, typename D::CC::D, typename D::II::second_type>>();

    return DF<I, C>(p, c_);
  }

  DataflowDependency<I> *op_;
  C *c_;
  std::function<I(typename C::D)> f_;
};

template <typename D> DF<D, ProcCollection<D>> distribute() {
  auto *p = Dataflow::dataflows.make<Distribute<D>>();
  auto *c = Collection::collections.make<ProcCollection<D>>();
  return DF<D, ProcCollection<D>>(p, c);
}

// template <typename D> DF<D> placeholder() {}

