#include "dataflow.hpp"

namespace nova {

template <typename I> struct DF {
  DF() {} // TODO
  DF(ProcDataflow<I> *op) : op_(op) {}

  using T = I;

  template <typename O, typename M> DF<O> map(M mapper) {
    auto *p = Dataflow::dataflows.make<Map<I, M, O>>(mapper);
    op_->dep.children.push_back(p);
    return DF<O>(p);
  }

  template <typename M> auto map(M mapper) {
    return map<decltype(mapper(*(I *)nullptr))>(mapper);
  }

  template <typename D, typename J> auto apply(D df, J join) {
    using MT = typename D::T::second_type;
    decltype(join((I *)nullptr, *(MT *)nullptr));
    static_assert(
        std::is_same<decltype(join((I *)nullptr, *(MT *)nullptr)), void>::value,
        "err");
    std::cout << "Msg type: " << typeid(MT).name() << std::endl;
    std::cout << "Obj type: " << typeid(I).name() << std::endl;

    auto *p = Dataflow::dataflows.make<Join<I, typename D::T, J>>(join);
    op_->dep.children.push_back(p);
    df.op_->dep.children.push_back(p);
    return DF<I>(p);
  }

  ProcDataflow<I> *op_;
};

template <typename D> DF<D> distribute() {
  auto *p = Dataflow::dataflows.make<Distribute<D>>();
  return DF<D>(p);
}

template <typename D> DF<D> placeholder() {}

} // namespace nova
