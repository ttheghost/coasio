#ifndef COASIO_TASK_HPP
#define COASIO_TASK_HPP

#include <coroutine>
#include <exception>
#include <optional>
#include <utility>

#include <asio/cancellation_signal.hpp>

namespace coasio {
template <typename T> class task {
public:
  struct promise_type {
    std::optional<T> result_;
    std::exception_ptr exception_;
    asio::cancellation_slot cancel_slot_;
    std::shared_ptr<asio::cancellation_signal> cancel_sig_;
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

    template <typename U>
    void return_value(U &&value)
      requires std::is_convertible_v<U, T>
    {
      result_.emplace(std::forward<U>(value));
    }
    void unhandled_exception() { exception_ = std::current_exception(); }

    template <typename U> auto await_transform(task<U> &&child) {
      child.handle().promise().cancel_slot_ = cancel_slot_;
      return std::move(child);
    }

    template <typename Awaitable>
    decltype(auto) await_transform(Awaitable &&a) {
      // Leaves (timers, sockets, etc.)
      if constexpr (requires { a.set_cancel_slot(cancel_slot_); }) {
        a.set_cancel_slot(cancel_slot_);
      }
      return std::forward<Awaitable>(a);
    }
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

  [[nodiscard]] std::coroutine_handle<promise_type> release() noexcept {
    return std::exchange(handle_, nullptr);
  }

  [[nodiscard]] std::coroutine_handle<promise_type> detach() noexcept {
    auto h = release();
    if (h)
      h.promise().detached_ = true;
    return h;
  }

  void set_cancellation_slot(asio::cancellation_slot slot) {
    handle_.promise().cancel_slot_ = slot;
  }

  void set_cancellation_signal(
      const std::shared_ptr<asio::cancellation_signal> &sig) {
    handle_.promise().cancel_sig_ = sig;
  }

  [[nodiscard]] std::coroutine_handle<promise_type> handle() const noexcept {
    return handle_;
  }

  explicit operator bool() const { return handle_ && !handle_.done(); }

private:
  std::coroutine_handle<promise_type> handle_;
};

template <> class task<void> {
public:
  struct promise_type {
    std::exception_ptr exception_;
    asio::cancellation_slot cancel_slot_;
    std::shared_ptr<asio::cancellation_signal> cancel_sig_;
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

    template <typename U> auto await_transform(task<U> &&child) {
      child.handle().promise().cancel_slot_ = cancel_slot_;
      return std::move(child);
    }

    template <typename Awaitable>
    decltype(auto) await_transform(Awaitable &&a) {
      // Leaves (timers, sockets, etc.)
      if constexpr (requires { a.set_cancel_slot(cancel_slot_); }) {
        a.set_cancel_slot(cancel_slot_);
      }
      return std::forward<Awaitable>(a);
    }
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

      void await_resume() const {
        auto &p = handle_.promise();
        if (p.exception_)
          std::rethrow_exception(p.exception_);
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

  [[nodiscard]] std::coroutine_handle<promise_type> release() noexcept {
    return std::exchange(handle_, nullptr);
  }

  [[nodiscard]] std::coroutine_handle<promise_type> detach() noexcept {
    auto h = release();
    if (h)
      h.promise().detached_ = true;
    return h;
  }

  void set_cancellation_slot(asio::cancellation_slot slot) {
    handle_.promise().cancel_slot_ = slot;
  }

  void set_cancellation_signal(
      const std::shared_ptr<asio::cancellation_signal> &sig) {
    handle_.promise().cancel_sig_ = sig;
  }

  [[nodiscard]] std::coroutine_handle<promise_type> handle() const noexcept {
    return handle_;
  }

  explicit operator bool() const { return handle_ && !handle_.done(); }

private:
  std::coroutine_handle<promise_type> handle_;
};
} // namespace coasio

#endif // !COASIO_TASK_HPP
