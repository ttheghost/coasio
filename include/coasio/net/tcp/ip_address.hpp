#ifndef COASIO_NET_IP_ADDRESS_HPP
#define COASIO_NET_IP_ADDRESS_HPP

#include <asio/ip/address.hpp>

namespace coasio::net::tcp {
class ipAddress {
public:
  ipAddress() = default;

  static ipAddress v4() noexcept {
    return ipAddress{asio::ip::address_v4::any()};
  }

  static ipAddress v6() noexcept {
    return ipAddress{asio::ip::address_v6::any()};
  }

  static ipAddress v4(const uint32_t addr) {
    return ipAddress{asio::ip::address_v4{addr}};
  }

  static ipAddress v6(const std::array<uint8_t, 16> &addr) {
    return ipAddress{asio::ip::address_v6{addr}};
  }

  static std::expected<ipAddress, std::error_code>
  from_string(std::string_view ip_string) noexcept {
    asio::error_code ec;
    const auto addr = asio::ip::make_address(ip_string, ec);
    if (ec) {
      return std::unexpected(ec);
    }
    return ipAddress{addr};
  }

  [[nodiscard]] bool is_v4() const noexcept { return address_.is_v4(); }

  [[nodiscard]] bool is_v6() const noexcept { return address_.is_v6(); }

  [[nodiscard]] bool is_loopback() const noexcept {
    return address_.is_loopback();
  }

  [[nodiscard]] bool is_multicast() const noexcept {
    return address_.is_multicast();
  }

  [[nodiscard]] auto is_unspecified() const noexcept {
    return address_.is_unspecified();
  }

  [[nodiscard]] std::string to_string() const { return address_.to_string(); }

  [[nodiscard]] ipAddress to_v4() const { return ipAddress{address_.to_v4()}; }

  [[nodiscard]] ipAddress to_v6() const { return ipAddress{address_.to_v6()}; }

  [[nodiscard]] auto &asio_address() noexcept { return address_; }

  std::ostream &operator<<(std::ostream &os) const { return os << address_; }

private:
  asio::ip::address address_{};

  explicit ipAddress(const asio::ip::address &address) noexcept
      : address_{address} {}
};
}; // namespace coasio::net::tcp

#endif // !COASIO_NET_IP_ADDRESS_HPP
