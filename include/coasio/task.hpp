#ifndef COASIO_TASK_HPP
#define COASIO_TASK_HPP

#include <coroutine>
#include <optional>
#include <exception>
#include <utility>

namespace coasio {
  template<typename T>
  class task {
  public:
    struct promise_type {
      std::coroutine_handle<> continuation_{nullptr};
      std::optional<T> result_;
      std::exception_ptr exception_;

      task get_return_object() {
        return task{std::coroutine_handle<promise_type>::from_promise(*this)};
      }

      std::suspend_always initial_suspend() { return {}; }

      auto final_suspend() noexcept {
        struct final_awaiter {
          bool await_ready() noexcept { return false; }

          std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
            return h.promise().continuation_ ? h.promise().continuation_ : std::noop_coroutine();
          }

          void await_resume() noexcept {
          }
        };
        return final_awaiter{};
      }

      void return_value(T value) { result_ = std::move(value); }
      void unhandled_exception() { exception_ = std::current_exception(); }
    };

    auto operator co_await() && noexcept {
      struct awaiter {
        std::coroutine_handle<promise_type> handle_;

        bool await_ready() const noexcept { return !handle_ || handle_.done(); }

        std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) noexcept {
          handle_.promise().continuation_ = caller;
          return handle_;
        }

        T await_resume() {
          if (handle_.promise().exception_) {
            auto exception = handle_.promise().exception_;
            handle_.destroy();
            std::rethrow_exception(exception);
          }
          T res = std::move(*handle_.promise().result_);
          handle_.destroy();
          return res;
        }
      };
      return awaiter{std::exchange(handle_, {})};
    }

    explicit task(std::coroutine_handle<promise_type> h) : handle_(h) {}
    ~task() { if (handle_) handle_.destroy(); }
    task(task &&other) noexcept : handle_(std::exchange(other.handle_, {})) {}

    T get() { return std::move(*handle_.promise().result_); }

    std::coroutine_handle<promise_type> handle() { return handle_; }

    std::coroutine_handle<promise_type> release() { return std::exchange(handle_, {}); }

  private:
    std::coroutine_handle<promise_type> handle_;
  };
}

#endif // !COASIO_TASK_HPP
