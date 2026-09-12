#ifndef COASIO_DETAIL_ASYNC_OP_HPP
#define COASIO_DETAIL_ASYNC_OP_HPP

#if defined(_MSC_VER) && !defined(__clang__)
#define NO_UNIQUE_ADDRESS [[msvc::no_unique_address]]
#elif defined(__has_cpp_attribute)
#if __has_cpp_attribute(no_unique_address)
#define NO_UNIQUE_ADDRESS [[no_unique_address]]
#else
#define NO_UNIQUE_ADDRESS
#endif
#else
#define NO_UNIQUE_ADDRESS
#endif

#include <assert.h>
#include <coroutine>
#include <expected>
#include <system_error>
#include <type_traits>
#include <utility>

#include <asio/bind_cancellation_slot.hpp>
#include <asio/cancellation_signal.hpp>

#include "coasio/runtime.hpp"

namespace coasio::detail {
template <typename T> struct result_storage {
  T value;
};

template <> struct result_storage<void> {};

template <typename Result, typename Initiator> class asio_awaitable {
  Initiator init_;
  std::error_code ec_{};
  asio::cancellation_slot cancel_slot_;
  NO_UNIQUE_ADDRESS result_storage<Result> store_{};

public:
  explicit asio_awaitable(Initiator init) noexcept(
      std::is_nothrow_move_constructible_v<Initiator>)
      : init_(std::move(init)) {}

  asio_awaitable(asio_awaitable &&) = default;

  asio_awaitable &operator=(asio_awaitable &&) = default;

  asio_awaitable(const asio_awaitable &) = delete;

  asio_awaitable &operator=(const asio_awaitable &) = delete;

  // See coasio::task::promise_type::await_transform(Awaitable &&a)
  void set_cancel_slot(asio::cancellation_slot slot) noexcept {
    cancel_slot_ = slot;
  }

  bool await_ready() const noexcept { return false; }

  void await_suspend(std::coroutine_handle<> h) noexcept {
    runtime *rt = runtime::current();
    assert(rt && "coasio: no current runtime bound to this thread");

    init_(asio::bind_cancellation_slot(
        cancel_slot_,
        [this, h, rt]<typename... Args>(const std::error_code &ec,
                                        Args &&...result) noexcept {
          ec_ = ec;
          if constexpr (!std::is_void_v<Result>) {
            if constexpr (sizeof...(result) > 0) {
              // drops all but the first completion handler argument like the
              // async_connect iterator (error_code, T, ignored...)
              store_.value = std::move(std::get<0>(
                  std::forward_as_tuple(std::forward<Args>(result)...)));
            }
          }
          rt->schedule(h);
        }));
  }

  [[nodiscard]] std::expected<Result, std::error_code> await_resume() noexcept {
    if (ec_)
      return std::unexpected(std::move(ec_));
    if constexpr (std::is_void_v<Result>)
      return {};
    else
      return std::move(store_.value);
  }
};

template <typename Result = void, typename Initiator>
[[nodiscard]] auto async_op(Initiator &&init) noexcept(
    std::is_nothrow_move_constructible_v<std::decay_t<Initiator>>) {
  return asio_awaitable<Result, std::decay_t<Initiator>>{
      std::forward<Initiator>(init)};
}
}; // namespace coasio::detail

#endif // !COASIO_DETAIL_ASYNC_OP_HPP
