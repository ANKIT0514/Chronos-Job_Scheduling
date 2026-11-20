#pragma once

#include "chronos/scheduler/in_memory_scheduler.hpp"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

namespace chronos {

struct ApiConfig {
    std::string api_key;
    std::size_t requests_per_minute;
    std::uint64_t max_job_runtime_ms;
    std::function<bool(const Job&)> submit_job;
    std::function<bool(const std::string&)> cancel_job;
    std::function<JobStatus(const std::string&)> query_job;

    ApiConfig()
        : requests_per_minute(60), max_job_runtime_ms(60000) {}
};

class ApiGateway {
public:
    ApiGateway(InMemoryScheduler& scheduler, const ApiConfig& config);
    ~ApiGateway();

    ApiGateway(const ApiGateway&) = delete;
    ApiGateway& operator=(const ApiGateway&) = delete;

    int start(const std::string& host, int port = 0);
    void listen(const std::string& host, int port);
    void stop();

private:
    bool authorize(
        const httplib::Request& request,
        httplib::Response& response);
    bool allow_request();
    void register_routes();
    static nlohmann::json status_json(const JobStatus& status);
    static void log_event(
        const std::string& event,
        const std::string& job_id,
        const std::string& state);

    InMemoryScheduler& scheduler_;
    const ApiConfig config_;
    httplib::Server server_;
    std::mutex rate_mutex_;
    std::chrono::steady_clock::time_point rate_window_start_;
    std::size_t requests_in_window_;
    std::thread server_thread_;
    std::atomic<bool> listen_failed_;
    std::atomic<std::uint64_t> requests_total_;
    std::atomic<std::uint64_t> rejected_total_;
    std::atomic<std::uint64_t> jobs_submitted_total_;
    std::atomic<std::uint64_t> jobs_cancelled_total_;
};

}
