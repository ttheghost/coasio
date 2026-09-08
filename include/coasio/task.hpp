#ifndef COASIO_TASK_HPP
#define COASIO_TASK_HPP

#include <coroutine>
#include <exception>
#include <optional>
#include <utility>

namespace coasio {
template <typename T> class task {
public:
  struct promise_type {
    std::optional<T> result_;
    std::exception_ptr exception_;
    std::coroutine_handle<> continuation_;
    bool detached_ = false;

    task get_return_object() {
      return task{std::coroutine_handle<promise_type>::from_promise(*this)};
    }

    std::suspend_always initial_suspend() { return {}; }

    auto final_suspend() noexcept {
      struct final_awaiter {
        bool await_ready() const noexcept { return false; }

        std::coroutine_handle<>
        await_suspend(std::coroutine_handle<promise_type> h) noexcept {
          auto &p = h.promise();
          if (p.detached_) {
            h.destroy();
            return std::noop_coroutine();
          }
          return (p.continuation_) ? p.continuation_ : std::noop_coroutine();
        }

        void await_resume() noexcept {}
      };
      return final_awaiter{};
    }

    void return_value(T value) { result_ = std::move(value); }
    void unhandled_exception() { exception_ = std::current_exception(); }
  };

  template <typename Self> auto operator co_await(this Self &&self) noexcept {
    static_assert(std::is_rvalue_reference_v<Self &&>,
                  "task must be co_awaited as a prvalue/rvalue, did you forget "
                  "std::move?");
    struct awaiter {
      std::coroutine_handle<promise_type> handle_;

      ~awaiter() {
        if (handle_)
          handle_.destroy();
      }

      bool await_ready() const noexcept { return !handle_ || handle_.done(); }

      std::coroutine_handle<>
      await_suspend(std::coroutine_handle<> caller) noexcept {
        handle_.promise().continuation_ = caller;
        return handle_;
      }

      T await_resume() {
        auto &p = handle_.promise();
        if (p.exception_)
          std::rethrow_exception(p.exception_);
        return std::move(*p.result_);
      }
    };
    return awaiter{std::exchange(self.handle_, nullptr)};
  }

  explicit task(std::coroutine_handle<promise_type> h) : handle_(h) {}

  task(task &&o) noexcept : handle_(std::exchange(o.handle_, {})) {}

  task(const task &) = delete;

  ~task() {
    if (handle_)
      handle_.destroy();
  }

  std::coroutine_handle<promise_type> release() noexcept {
    return std::exchange(handle_, nullptr);
  }

  std::coroutine_handle<promise_type> detach() noexcept {
    auto h = release();
    if (h)
      h.promise().detached_ = true;
    return h;
  }

  std::coroutine_handle<> handle() const noexcept { return handle_; }

private:
  std::coroutine_handle<promise_type> handle_;
};

template <> class task<void> {
public:
  struct promise_type {
    std::exception_ptr exception_;
    std::coroutine_handle<> continuation_;
    bool detached_ = false;

    task get_return_object() {
      return task{std::coroutine_handle<promise_type>::from_promise(*this)};
    }

    std::suspend_always initial_suspend() { return {}; }

    auto final_suspend() noexcept {
      struct final_awaiter {
        bool await_ready() const noexcept { return false; }

        std::coroutine_handle<>
        await_suspend(std::coroutine_handle<promise_type> h) noexcept {
          auto &p = h.promise();
          if (p.detached_) {
            h.destroy();
            return std::noop_coroutine();
          }
          return (p.continuation_) ? p.continuation_ : std::noop_coroutine();
        }

        void await_resume() noexcept {}
      };
      return final_awaiter{};
    }

    void return_void() {}

    void unhandled_exception() { exception_ = std::current_exception(); }
  };

  auto operator co_await() && noexcept {
    struct awaiter {
      std::coroutine_handle<promise_type> handle_;

      ~awaiter() {
        if (handle_)
          handle_.destroy();
      }

      bool await_ready() const noexcept { return !handle_ || handle_.done(); }

      std::coroutine_handle<>
      await_suspend(std::coroutine_handle<> caller) noexcept {
        handle_.promise().continuation_ = caller;
        return handle_;
      }

      void await_resume() const {
        auto &p = handle_.promise();
        if (p.exception_)
          std::rethrow_exception(p.exception_);
      }
    };
    return awaiter{std::exchange(handle_, nullptr)};
  }

  explicit task(std::coroutine_handle<promise_type> h) : handle_(h) {}

  task(task &&o) noexcept : handle_(std::exchange(o.handle_, {})) {}

  task(const task &) = delete;

  ~task() {
    if (handle_)
      handle_.destroy();
  }

  std::coroutine_handle<promise_type> release() noexcept {
    return std::exchange(handle_, nullptr);
  }

  std::coroutine_handle<promise_type> detach() noexcept {
    auto h = release();
    if (h)
      h.promise().detached_ = true;
    return h;
  }

  std::coroutine_handle<> handle() const noexcept { return handle_; }

private:
  std::coroutine_handle<promise_type> handle_;
};
} // namespace coasio

#endif // !COASIO_TASK_HPP
