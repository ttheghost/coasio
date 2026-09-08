#include <coasio/net/tcp/socket.hpp>
#include <coasio/runtime.hpp>
#include <coasio/task.hpp>
#include <iostream>

#include "coasio/time.hpp"

int main() {
  coasio::runtime rt;

  rt.block_on([]() -> coasio::task<void> {
    auto maybeStream =
        co_await coasio::net::tcp::tcpSocket::connect_to("127.0.0.1:8080");
    if (!maybeStream) {
      std::cout << "failed to connect: " << maybeStream.error().message()
                << "\n";
      co_return;
    }
    std::cout << "connected\n";
  }());

  std::cout << "runtime stopped\n";
}
