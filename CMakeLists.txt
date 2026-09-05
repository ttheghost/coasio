cmake_minimum_required(VERSION 3.24)
project(coasio CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)
FetchContent_Declare(
        asio
        GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
        GIT_TAG asio-1-38-2
        GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(asio)

add_library(asio INTERFACE)
target_include_directories(asio INTERFACE ${asio_SOURCE_DIR}/include)
target_compile_definitions(asio INTERFACE ASIO_STANDALONE ASIO_NO_DEPRECATED)

add_subdirectory(src)
add_subdirectory(examples)
