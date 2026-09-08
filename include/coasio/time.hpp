#ifndef COASIO_TIME_HPP
#define COASIO_TIME_HPP

#include "runtime.hpp"
#include <asio.hpp>
#include <chrono>
#include <coroutine>
#include <expected>

namespace coasio::time {
inline auto sleep(const std::chrono::milliseconds ms) {
  struct sleep_awaiter {
    std::error_code ec_;
    std::chrono::milliseconds duration_;
    std::unique_ptr<asio::steady_timer> timer_;

    explicit sleep_awaiter(const std::chrono::milliseconds duration)
        : duration_(duration) {}

    bool await_ready() const noexcept { return duration_.count() <= 0; }

    void await_suspend(std::coroutine_handle<> h) noexcept {
      runtime *rt = runtime::current();
      if (!rt) {
        std::cout << "Called outside a coasio runtime\n";
        std::terminate();
      }
      timer_ =
          std::make_unique<asio::steady_timer>(rt->get_io_context(), duration_);
      timer_->async_wait([h, rt, this](const asio::error_code &ec) {
        ec_ = ec;
        rt->schedule(h);
      });
    }

    std::expected<void, std::error_code> await_resume() noexcept {
      if (ec_)
        return std::unexpected(std::move(ec_));
      return {};
    }
  };

  return sleep_awaiter{ms};
}
}; // namespace coasio::time

#endif // !COASIO_TIME_HPP
