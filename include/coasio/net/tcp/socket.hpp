#ifndef COASIO_NET_TCP_SOCKET_HPP
#define COASIO_NET_TCP_SOCKET_HPP
#include <expected>
#include <span>
#include <string>
#include <string_view>

#include <asio/connect.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read.hpp>
#include <asio/write.hpp>

#include "coasio/net/helper.hpp"
#include "endpoint.hpp"
#include "resolver.hpp"

namespace coasio::net::tcp {
class socket {
  auto connect_impl(const asio::ip::tcp::resolver::results_type &endpoints) {
    struct connect_any_awaiter {
      std::error_code ec_;
      socket &socket_;
      asio::ip::tcp::resolver::results_type endpoints_;

      explicit connect_any_awaiter(
          socket &sck, const asio::ip::tcp::resolver::results_type &eps)
          : socket_{sck}, endpoints_{eps} {}

      bool await_ready() const noexcept { return false; }

      void await_suspend(std::coroutine_handle<> h) noexcept {
        auto *rt = runtime::current();
        asio::async_connect(
            socket_.asio_handle(), endpoints_.begin(), endpoints_.end(),
            [h, rt, this](const asio::error_code &ec, auto /*iterator*/) {
              ec_ = ec;
              rt->schedule(h);
            });
      }

      std::expected<void, std::error_code> await_resume() noexcept {
        if (ec_)
          return std::unexpected(ec_);
        return {};
      }
    };

    return connect_any_awaiter{*this, endpoints};
  }

public:
  using wait_type = asio::ip::tcp::socket::wait_type;
  using shutdown_type = asio::ip::tcp::socket::shutdown_type;

  explicit socket(asio::ip::tcp::socket socket) noexcept
      : socket_{std::move(socket)} {}

  static socket create() noexcept {
    asio::ip::tcp::socket sck(runtime::get_current_io_context());
    return socket{std::move(sck)};
  }

  static task<std::expected<socket, std::error_code>>
  connect_to(std::string_view host_and_port) {
    auto maybe_hap = parse_host_port(host_and_port);
    if (!maybe_hap) {
      co_return std::unexpected(maybe_hap.error());
    }
    if (!maybe_hap.value().port.has_value()) {
      co_return std::unexpected(
          std::make_error_code(std::errc::invalid_argument));
    }
    auto [host, port] = maybe_hap.value();

    resolver r = resolver::create();
    auto maybe_eps = co_await r.resolve(host, std::to_string(port.value()));
    if (!maybe_eps) {
      co_return std::unexpected(maybe_eps.error());
    }

    auto sck = socket::create();
    auto result = co_await sck.connect_impl(*maybe_eps);
    if (!result) {
      co_return std::unexpected(result.error());
    }
    co_return std::move(sck);
  }

  auto connect(const endpoint &ep) {
    struct connect_awaiter {
      std::error_code ec_;
      socket &socket_;
      endpoint ep_;

      explicit connect_awaiter(socket &socket, endpoint ep)
          : socket_{socket}, ep_{ep} {}

      bool await_ready() const noexcept { return false; }

      void await_suspend(std::coroutine_handle<> h) noexcept {
        auto *rt = runtime::current();
        socket_.asio_handle().async_connect(
            ep_.asio_endpoint(), [h, rt, this](const asio::error_code &ec) {
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

    return connect_awaiter{*this, ep};
  }

  std::expected<void, std::error_code> bind(const endpoint &ep) {
    std::error_code ec_;
    if (!socket_.is_open()) {
      socket_.open(ep.asio_endpoint().protocol(), ec_);
      if (ec_)
        return std::unexpected(ec_);
    }
    socket_.bind(ep.asio_endpoint(), ec_);
    if (ec_)
      return std::unexpected(ec_);
    return {};
  }

  auto read(std::span<std::byte> buffer) {
    struct read_awaiter {
      std::error_code ec_;
      std::span<std::byte> buffer_;
      socket &socket_;
      std::size_t bytes_read_ = 0;

      explicit read_awaiter(socket &socket, std::span<std::byte> buffer)
          : socket_(socket), buffer_(buffer) {}

      bool await_ready() const noexcept { return false; }

      void await_suspend(std::coroutine_handle<> h) noexcept {
        runtime *rt = runtime::current();
        asio::async_read(socket_.asio_handle(), asio::buffer(buffer_),
                         [h, rt, this](const asio::error_code &ec,
                                       std::size_t bytes_transferred) {
                           ec_ = ec;
                           bytes_read_ = bytes_transferred;
                           rt->schedule(h);
                         });
      }

      std::expected<size_t, std::error_code> await_resume() noexcept {
        if (ec_)
          return std::unexpected(std::move(ec_));
        return bytes_read_;
      }
    };

    return read_awaiter{*this, buffer};
  }

  auto read_some(std::span<std::byte> buffer) {
    struct read_some_awaiter {
      std::error_code ec_;
      std::span<std::byte> buffer_;
      socket &socket_;
      std::size_t bytes_read_ = 0;

      explicit read_some_awaiter(socket &socket, std::span<std::byte> buffer)
          : socket_(socket), buffer_(buffer) {}

      bool await_ready() const noexcept { return false; }

      void await_suspend(std::coroutine_handle<> h) noexcept {
        runtime *rt = runtime::current();
        socket_.asio_handle().async_read_some(
            asio::buffer(buffer_),
            [h, rt, this](const asio::error_code &ec,
                          std::size_t bytes_transferred) {
              ec_ = ec;
              bytes_read_ = bytes_transferred;
              rt->schedule(h);
            });
      }

      std::expected<size_t, std::error_code> await_resume() noexcept {
        if (ec_)
          return std::unexpected(std::move(ec_));
        return bytes_read_;
      }
    };

    return read_some_awaiter{*this, buffer};
  }

  // TODO: read_until

  auto write(std::span<const std::byte> buffer) {
    struct write_awaiter {
      std::error_code ec_;
      std::span<const std::byte> buffer_;
      socket &socket_;
      std::size_t bytes_written_ = 0;

      explicit write_awaiter(socket &socket, std::span<const std::byte> buffer)
          : socket_(socket), buffer_(buffer) {}

      bool await_ready() const noexcept { return false; }

      void await_suspend(std::coroutine_handle<> h) noexcept {
        runtime *rt = runtime::current();
        asio::async_write(socket_.asio_handle(), asio::buffer(buffer_),
                          [h, rt, this](const asio::error_code &ec,
                                        std::size_t bytes_transferred) {
                            ec_ = ec;
                            bytes_written_ = bytes_transferred;
                            rt->schedule(h);
                          });
      }

      std::expected<size_t, std::error_code> await_resume() noexcept {
        if (ec_)
          return std::unexpected(std::move(ec_));
        return bytes_written_;
      }
    };

    return write_awaiter{*this, buffer};
  }

  auto write_some(std::span<const std::byte> buffer) {
    struct write_some_awaiter {
      std::error_code ec_;
      std::span<const std::byte> buffer_;
      socket &socket_;
      std::size_t bytes_written_ = 0;

      explicit write_some_awaiter(socket &socket,
                                  std::span<const std::byte> buffer)
          : socket_(socket), buffer_(buffer) {}

      bool await_ready() const noexcept { return false; }

      void await_suspend(std::coroutine_handle<> h) noexcept {
        runtime *rt = runtime::current();
        socket_.asio_handle().async_write_some(
            asio::buffer(buffer_),
            [h, rt, this](const asio::error_code &ec,
                          std::size_t bytes_transferred) {
              ec_ = ec;
              bytes_written_ = bytes_transferred;
              rt->schedule(h);
            });
      }

      std::expected<size_t, std::error_code> await_resume() noexcept {
        if (ec_)
          return std::unexpected(std::move(ec_));
        return bytes_written_;
      }
    };

    return write_some_awaiter{*this, buffer};
  }

  auto wait(wait_type type) {
    struct wait_awaiter {
      std::error_code ec_;
      socket &socket_;
      wait_type type_;

      explicit wait_awaiter(socket &sck, wait_type type)
          : socket_{sck}, type_{type} {}

      bool await_ready() const noexcept { return false; }

      void await_suspend(std::coroutine_handle<> h) noexcept {
        runtime *rt = runtime::current();
        socket_.asio_handle().async_wait(
            type_, [h, rt, this](const asio::error_code &ec) {
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

    return wait_awaiter{*this, type};
  }

  std::expected<size_t, std::error_code> available() const {
    std::error_code ec_;
    size_t bytes_available = socket_.available(ec_);
    if (ec_)
      return std::unexpected(std::move(ec_));
    return bytes_available;
  }

  std::expected<void, std::error_code> shutdown(const shutdown_type type) {
    std::error_code ec_;
    socket_.shutdown(type, ec_);
    if (ec_)
      return std::unexpected(std::move(ec_));
    return {};
  }

  std::expected<void, std::error_code> close() {
    std::error_code ec_;
    socket_.close(ec_);
    if (ec_)
      return std::unexpected(std::move(ec_));
    return {};
  }

  bool is_open() const { return socket_.is_open(); }

  std::expected<endpoint, std::error_code> remote_endpoint() const noexcept {
    asio::error_code ec;
    auto ep = socket_.remote_endpoint(ec);
    if (ec)
      return std::unexpected{ec};
    return endpoint{ep};
  }

  std::expected<endpoint, std::error_code> local_endpoint() const noexcept {
    asio::error_code ec;
    auto ep = socket_.local_endpoint(ec);
    if (ec)
      return std::unexpected{ec};
    return endpoint{ep};
  }

  auto native_handle() noexcept { return socket_.native_handle(); }

  asio::ip::tcp::socket &asio_handle() noexcept { return socket_; }

private:
  asio::ip::tcp::socket socket_;
};
}; // namespace coasio::net::tcp

#endif // !COASIO_NET_TCP_SOCKET_HPP
