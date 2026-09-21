#ifndef COASIO_HPP
#define COASIO_HPP

#include "coasio/runtime.hpp"
#include "coasio/task.hpp"

namespace coasio {

template <typename Arg> auto spawn(Arg &&arg) {
  runtime *rt = runtime::current();
  if (!rt) {
    std::printf("coasio::spawn() called outside a coasio runtime; "
                "did you mean rt.spawn(...) instead?\n");
    std::terminate();
  }
  return rt->spawn(std::forward<Arg>(arg));
}

}; // namespace coasio

#endif // !COASIO_HPP
