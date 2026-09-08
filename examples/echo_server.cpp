#include <coasio/net/tcp/socket.hpp>
#include <coasio/runtime.hpp>
#include <coasio/task.hpp>
#include <print>
#include <span>
#include <string_view>
#include <vector>

#include "coasio/time.hpp"

int main() {
  coasio::runtime rt;

  rt.block_on([]() -> coasio::task<void> {
    auto maybeStream =
        co_await coasio::net::tcp::socket::connect_to("www.google.com:80");
    if (!maybeStream) {
      std::println(std::cerr, "failed to connect: {}",
                   maybeStream.error().message());
      co_return;
    }
    auto stream = std::move(*maybeStream);
    std::println("connected");

    constexpr std::string_view httpRequest = "GET / HTTP/1.1\r\n"
                                             "Host: www.google.com\r\n"
                                             "User-Agent: coasio-client/1.0\r\n"
                                             "Accept: */*\r\n"
                                             "Connection: close\r\n\r\n";

    auto writeBuffer = std::as_bytes(std::span{httpRequest});

    auto maybeWriteSize = co_await stream.write(writeBuffer);
    if (!maybeWriteSize) {
      std::println(std::cerr, "failed to write request: {}",
                   maybeWriteSize.error().message());
      co_return;
    }
    std::println("Sent HTTP Request ({} bytes)", *maybeWriteSize);

    std::vector<std::byte> buffer{4096};

    auto maybeSize = co_await stream.read_some(buffer);
    if (!maybeSize) {
      std::println(std::cerr, "failed to read: {}",
                   maybeSize.error().message());
      co_return;
    }

    size_t bytesRead = *maybeSize;
    std::println("Read {} bytes from server\n", bytesRead);

    std::string_view httpResponse{reinterpret_cast<const char *>(buffer.data()),
                                  bytesRead};

    std::cout << "--- Received HTTP Frame ---\n";
    std::println("{}", httpResponse);
    std::cout << "---------------------------\n";
  }());

  std::cout << "runtime stopped\n";
}
