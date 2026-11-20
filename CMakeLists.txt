cmake_minimum_required(VERSION 3.16)
project(Chronos VERSION 0.1.0 LANGUAGES CXX)

option(CHRONOS_ENABLE_POSTGRES "Build PostgreSQL persistence support" OFF)
option(CHRONOS_ENABLE_REDIS "Build Redis worker-state support" OFF)
option(CHRONOS_ENABLE_KAFKA "Build Kafka event-stream support" OFF)

if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(CTest)
find_package(Threads REQUIRED)

add_library(chronos_options INTERFACE)
target_include_directories(chronos_options INTERFACE
    "${CMAKE_CURRENT_SOURCE_DIR}/include")

add_library(chronos_core
    src/job_priority_queue.cpp
    src/job_state.cpp
    src/in_memory_scheduler.cpp
    src/worker_selector.cpp
    src/retry_policy.cpp
    src/worker_manager.cpp
    src/workflow_graph.cpp
    src/workflow_executor.cpp)
target_link_libraries(chronos_core PUBLIC chronos_options Threads::Threads)

add_executable(chronos src/main.cpp)
target_link_libraries(chronos PRIVATE chronos_core)

add_executable(chronos_benchmark src/benchmark_main.cpp)
target_link_libraries(chronos_benchmark PRIVATE chronos_core)
if(WIN32)
    target_link_libraries(chronos_benchmark PRIVATE advapi32)
endif()

include(FetchContent)
set(HTTPLIB_REQUIRE_OPENSSL OFF CACHE BOOL "" FORCE)
set(HTTPLIB_USE_ZLIB_IF_AVAILABLE OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    cpp-httplib
    URL https://github.com/yhirose/cpp-httplib/archive/refs/tags/v0.18.6.zip)
FetchContent_Declare(
    nlohmann_json
    URL https://github.com/nlohmann/json/archive/refs/tags/v3.11.3.zip)
FetchContent_MakeAvailable(cpp-httplib nlohmann_json)

add_library(chronos_api src/api_gateway.cpp)
target_link_libraries(chronos_api
    PUBLIC chronos_core httplib nlohmann_json::nlohmann_json)

add_executable(chronos_api_gateway src/api_main.cpp)
target_link_libraries(chronos_api_gateway PRIVATE chronos_api)

if(CHRONOS_ENABLE_POSTGRES)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(PQXX REQUIRED IMPORTED_TARGET libpqxx)
    if(PQXX_VERSION VERSION_GREATER_EQUAL 8.0)
        message(FATAL_ERROR
            "Chronos uses C++17; libpqxx 8.x requires C++20. Install libpqxx 7.x.")
    endif()

    add_library(chronos_postgres src/job_repository.cpp)
    target_link_libraries(chronos_postgres
        PUBLIC chronos_core PkgConfig::PQXX)

    add_executable(chronos_postgres_demo src/postgres_demo.cpp)
    target_link_libraries(chronos_postgres_demo PRIVATE chronos_postgres)
endif()

if(CHRONOS_ENABLE_REDIS)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(HIREDIS REQUIRED IMPORTED_TARGET hiredis)

    add_library(chronos_redis src/redis_worker_store.cpp)
    target_link_libraries(chronos_redis
        PUBLIC chronos_core PkgConfig::HIREDIS)
endif()

if(CHRONOS_ENABLE_KAFKA)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(RDKAFKA REQUIRED IMPORTED_TARGET rdkafka)

    add_library(chronos_kafka src/kafka_event_bus.cpp)
    target_link_libraries(chronos_kafka
        PUBLIC chronos_core PkgConfig::RDKAFKA)
endif()

if(CHRONOS_ENABLE_KAFKA)
    add_executable(chronos_worker_agent src/worker_agent_main.cpp)
    target_link_libraries(chronos_worker_agent
        PRIVATE chronos_kafka nlohmann_json::nlohmann_json)
endif()

if(CHRONOS_ENABLE_KAFKA AND CHRONOS_ENABLE_REDIS)
    add_executable(chronos_worker_manager_service
        src/worker_manager_service_main.cpp)
    target_link_libraries(chronos_worker_manager_service
        PRIVATE chronos_kafka chronos_redis nlohmann_json::nlohmann_json)
endif()

if(CHRONOS_ENABLE_KAFKA AND CHRONOS_ENABLE_POSTGRES)
    target_link_libraries(chronos_api PRIVATE chronos_kafka chronos_postgres)
    target_compile_definitions(chronos_api PUBLIC CHRONOS_DISTRIBUTED_BACKEND)

    add_executable(chronos_scheduler_service src/scheduler_service_main.cpp)
    target_link_libraries(chronos_scheduler_service
        PRIVATE chronos_kafka chronos_postgres nlohmann_json::nlohmann_json)
endif()

if(BUILD_TESTING)
    find_package(GTest CONFIG QUIET)
    if(NOT GTest_FOUND)
        set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
        FetchContent_Declare(
            googletest
            URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip)
        FetchContent_MakeAvailable(googletest)
    endif()

    add_executable(chronos_tests
        tests/job_priority_queue_test.cpp
        tests/in_memory_scheduler_test.cpp
        tests/worker_selector_test.cpp
        tests/retry_policy_test.cpp
        tests/worker_manager_test.cpp
        tests/workflow_graph_test.cpp)
    target_link_libraries(chronos_tests PRIVATE chronos_core GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(chronos_tests)

    add_executable(chronos_api_tests tests/api_gateway_test.cpp)
    target_link_libraries(chronos_api_tests
        PRIVATE chronos_api GTest::gtest_main)
    gtest_discover_tests(chronos_api_tests)

    if(CHRONOS_ENABLE_POSTGRES)
        add_executable(chronos_postgres_tests
            tests/postgres_repository_test.cpp)
        target_link_libraries(chronos_postgres_tests
            PRIVATE chronos_postgres GTest::gtest_main)
        target_compile_definitions(chronos_postgres_tests PRIVATE
            CHRONOS_SOURCE_DIR="${PROJECT_SOURCE_DIR}")
        gtest_discover_tests(chronos_postgres_tests)
    endif()

    if(CHRONOS_ENABLE_REDIS)
        add_executable(chronos_redis_tests tests/redis_worker_store_test.cpp)
        target_link_libraries(chronos_redis_tests
            PRIVATE chronos_redis GTest::gtest_main)
        gtest_discover_tests(chronos_redis_tests)
    endif()

    if(CHRONOS_ENABLE_KAFKA)
        add_executable(chronos_kafka_tests tests/kafka_event_bus_test.cpp)
        target_link_libraries(chronos_kafka_tests
            PRIVATE chronos_kafka GTest::gtest_main)
        gtest_discover_tests(chronos_kafka_tests)
    endif()
endif()
