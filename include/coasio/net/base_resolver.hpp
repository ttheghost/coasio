#ifndef COASIO_NET_BASE_RESOLVER_HPP
#define COASIO_NET_BASE_RESOLVER_HPP
#include <expected>
#include <string>
#include <string_view>

#include <asio/ip/basic_resolver.hpp>

#include "coasio/runtime.hpp"

namespace coasio::net {
template <typename InternetProtocol> class base_resolver {
public:
  typedef InternetProtocol protocol_type;
  using results_type = asio::ip::basic_resolver<protocol_type>::results_type;

  static base_resolver create() noexcept {
    return base_resolver{runtime::get_current_io_context()};
  }

  auto resolve(std::string_view host, std::string_view service) {
    struct resolve_awaiter {
      std::error_code ec_;
      std::string host_;
      std::string service_;
      results_type results_;
      base_resolver &resolver_;

      resolve_awaiter(base_resolver &resolver, std::string_view host,
                      std::string_view service)
          : host_(host), service_(service), resolver_(resolver) {}

      bool await_ready() const noexcept { return false; }

      void await_suspend(std::coroutine_handle<> h) noexcept {
        runtime *rt = runtime::current();
        if (!rt) {
          std::cout << "Called resolve outside a coasio runtime\n";
          std::terminate();
        }

        resolver_.asio_handle().async_resolve(
            host_, service_,
            [h, rt, this](const asio::error_code &ec, results_type results) {
              ec_ = ec;
              results_ = std::move(results);
              rt->schedule(h);
            });
      }

      std::expected<results_type, std::error_code>
      await_resume() const noexcept {
        if (ec_)
          return std::unexpected(ec_);
        return std::move(results_);
      }
    };

    return resolve_awaiter{*this, host, service};
  }

  auto &asio_handle() noexcept { return resolver_; }

private:
  explicit base_resolver(asio::io_context &io_context) noexcept
      : resolver_{io_context} {}

  asio::ip::basic_resolver<protocol_type> resolver_;
};
}; // namespace coasio::net

#endif // !COASIO_NET_BASE_RESOLVER_HPP
