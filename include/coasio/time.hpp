#ifndef COASIO_TIME_HPP
#define COASIO_TIME_HPP

#include "detail/async_op.hpp"
#include "runtime.hpp"
#include <asio/basic_waitable_timer.hpp>
#include <asio/error_code.hpp>
#include <asio/steady_timer.hpp>
#include <chrono>
#include <coroutine>
#include <expected>

namespace coasio::time {
template <typename Clock = std::chrono::steady_clock> class timer {
public:
  using duration = Clock::duration;
  using time_point = Clock::time_point;

  explicit timer(asio::basic_waitable_timer<Clock> timer)
      : timer_(std::move(timer)) {}

  // creates a timer without setting an expiry time
  static timer create() noexcept {
    return timer{
        asio::basic_waitable_timer<Clock>{runtime::get_current_io_context()}};
  }

  // creates a timer and sets the expiry time as an absolute time.
  static timer create(const time_point &expiry_time) noexcept {
    return timer{asio::basic_waitable_timer<Clock>{
        runtime::get_current_io_context(), expiry_time}};
  }

  // creates a timer and sets the expiry time relative to now.
  static timer create(const duration &expiry_time) noexcept {
    return timer{asio::basic_waitable_timer<Clock>{
        runtime::get_current_io_context(), expiry_time}};
  }

  /**
   * @throws asio::system_error Thrown on failure.
   */
  size_t cancel() { return timer_.cancel(); }

  /**
   * @throws asio::system_error Thrown on failure.
   */
  size_t cancel_one() { return timer_.cancel_one(); }

  /**
   * @throws asio::system_error Thrown on failure.
   */
  size_t expires_after(const duration &expiry_time) {
    return timer_.expires_after(expiry_time);
  }

  size_t expires_at(const time_point &expiry_time) {
    return timer_.expires_at(expiry_time);
  }

  time_point expiry() const { return timer_.expiry(); }

  auto wait() const noexcept {
    return detail::async_op<void>([this]<typename Args>(Args &&token) {
      asio_handle().async_wait(std::forward<Args>(token));
    });
  }

  asio::basic_waitable_timer<Clock> &asio_handle() noexcept { return timer_; }

private:
  asio::basic_waitable_timer<Clock> timer_;
};

inline auto sleep(const std::chrono::milliseconds ms) {
  struct sleep_awaiter {
    std::error_code ec_;
    std::chrono::milliseconds duration_;
    asio::cancellation_slot cancel_slot_;
    std::unique_ptr<asio::steady_timer> timer_;

    // See coasio::task::promise_type::await_transform(Awaitable &&a)
    void set_cancel_slot(asio::cancellation_slot slot) noexcept {
      cancel_slot_ = slot;
    }

    explicit sleep_awaiter(const std::chrono::milliseconds duration)
        : duration_(duration) {}

    bool await_ready() const noexcept { return duration_.count() <= 0; }

    void await_suspend(std::coroutine_handle<> h) noexcept {
      runtime *rt = runtime::current();
      if (!rt) {
        std::cerr << "Called outside a coasio runtime\n";
        std::terminate();
      }
      timer_ =
          std::make_unique<asio::steady_timer>(rt->get_io_context(), duration_);
      timer_->async_wait(asio::bind_cancellation_slot(
          cancel_slot_, [h, rt, this](const asio::error_code &ec) {
            ec_ = ec;
            rt->schedule(h);
          }));
    }

    std::expected<void, std::error_code> await_resume() noexcept {
      if (ec_)
        return std::unexpected(ec_);
      return {};
    }
  };

  return sleep_awaiter{ms};
}
}; // namespace coasio::time

#endif // !COASIO_TIME_HPP
