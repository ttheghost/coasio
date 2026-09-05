#ifndef COASIO_TIME_HPP
#define COASIO_TIME_HPP

#include "runtime.hpp"
#include <asio.hpp>
#include <chrono>
#include <coroutine>

namespace coasio::time {
  inline auto sleep(const std::chrono::milliseconds ms) {
    struct sleep_awaiter {
      std::chrono::milliseconds duration_;
      std::unique_ptr<asio::steady_timer> timer_;

      explicit sleep_awaiter(const std::chrono::milliseconds duration) : duration_(duration) {
      }

      bool await_ready() const noexcept {
        return duration_.count() <= 0;
      }

      void await_suspend(std::coroutine_handle<> h) {
        runtime *rt = runtime::current();
        if (!rt) {
          std::cout << "Called outside a coasio runtime\n";
          std::terminate();
        }
        timer_ = std::make_unique<asio::steady_timer>(rt->get_io_context(), duration_);
        timer_->async_wait([h, rt](const asio::error_code &ec) {
          if (!ec) {
              rt->schedule(h);
          }
        });
      }

      void await_resume() const noexcept {
      }
    };

    return sleep_awaiter{ms};
  }
};

#endif // !COASIO_TIME_HPP
