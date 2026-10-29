cmake_minimum_required(VERSION 3.16)
project(datapath_ds C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

find_package(Threads REQUIRED)

set(SOURCES driver.c flowtable.c ring.h flowtable.h)
set(WARNINGS -Wall -Wextra -Wpedantic)

# Normal build: run/debug this one.
add_executable(driver ${SOURCES})
target_compile_options(driver PRIVATE ${WARNINGS})
target_link_libraries(driver PRIVATE Threads::Threads)

# Sanitizer builds aren't supported by MinGW on Windows; use WSL/Linux/macOS for these.
if(NOT WIN32)
    # AddressSanitizer + UndefinedBehaviorSanitizer (run with args: test --quick)
    add_executable(driver-asan ${SOURCES})
    target_compile_options(driver-asan PRIVATE ${WARNINGS} -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(driver-asan PRIVATE -fsanitize=address,undefined)
    target_link_libraries(driver-asan PRIVATE Threads::Threads)

    # ThreadSanitizer (run with args: test --quick)
    add_executable(driver-tsan ${SOURCES})
    target_compile_options(driver-tsan PRIVATE ${WARNINGS} -O1 -g -fsanitize=thread)
    target_link_options(driver-tsan PRIVATE -fsanitize=thread)
    target_link_libraries(driver-tsan PRIVATE Threads::Threads)
endif()
