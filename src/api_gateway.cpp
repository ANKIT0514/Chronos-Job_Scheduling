#include "chronos/api/api_gateway.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <limits>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <mutex>

namespace chronos {
namespace {

typedef nlohmann::json Json;

std::mutex& log_mutex() {
    static std::mutex mutex;
    return mutex;
}

bool valid_id(const std::string& id) {
    if (id.empty() || id.size() > 64) {
        return false;
    }
    return std::regex_match(id, std::regex("[A-Za-z0-9_.-]+"));
}

bool nonnegative_integer(const Json& value) {
    if (!value.is_number_integer()) {
        return false;
    }
    return value.is_number_unsigned() ||
           value.get<std::int64_t>() >= 0;
}

std::uint64_t read_unsigned(
    const Json& value,
    std::uint64_t maximum,
    const std::string& field) {
    if (!nonnegative_integer(value)) {
        throw std::invalid_argument(field + " must be a non-negative integer.");
    }
    const std::uint64_t parsed = value.get<std::uint64_t>();
    if (parsed > maximum) {
        throw std::invalid_argument(field + " exceeds its maximum value.");
    }
    return parsed;
}

bool constant_time_equal(
    const std::string& expected,
    const std::string& actual) {
    std::size_t difference = expected.size() ^ actual.size();
    const std::size_t length = std::max(expected.size(), actual.size());
    for (std::size_t i = 0; i < length; ++i) {
        const unsigned char left = i < expected.size()
            ? static_cast<unsigned char>(expected[i]) : 0;
        const unsigned char right = i < actual.size()
            ? static_cast<unsigned char>(actual[i]) : 0;
        difference |= static_cast<std::size_t>(left ^ right);
    }
    return difference == 0;
}

void set_json(
    httplib::Response& response,
    int status,
    const Json& body) {
    response.status = status;
    response.set_content(body.dump(), "application/json");
}

}

ApiGateway::ApiGateway(
    InMemoryScheduler& scheduler,
    const ApiConfig& config)
    : scheduler_(scheduler),
      config_(config),
      rate_window_start_(std::chrono::steady_clock::now()),
      requests_in_window_(0),
      listen_failed_(false),
      requests_total_(0),
      rejected_total_(0),
      jobs_submitted_total_(0),
      jobs_cancelled_total_(0) {
    if (config_.api_key.empty()) {
        throw std::invalid_argument("The API key must not be empty.");
    }
    if (config_.requests_per_minute == 0) {
        throw std::invalid_argument("The request limit must be positive.");
    }
    if (config_.max_job_runtime_ms == 0) {
        throw std::invalid_argument("The maximum job runtime must be positive.");
    }
    server_.set_payload_max_length(1024 * 1024);
    register_routes();
}

ApiGateway::~ApiGateway() {
    if (server_thread_.joinable()) {
        server_.stop();
        server_thread_.join();
    }
}

int ApiGateway::start(const std::string& host, int port) {
    if (server_thread_.joinable()) {
        throw std::logic_error("The API gateway is already started.");
    }
    const int bound_port = server_.bind_to_any_port(host, port);
    if (bound_port == 0) {
        throw std::runtime_error("Could not bind the API gateway listener.");
    }
    listen_failed_.store(false);
    server_thread_ = std::thread([this]() {
        if (!server_.listen_after_bind()) {
            listen_failed_.store(true);
        }
    });
    return bound_port;
}

void ApiGateway::listen(const std::string& host, int port) {
    if (!server_.listen(host, port)) {
        throw std::runtime_error("Could not listen on the API gateway address.");
    }
}

void ApiGateway::stop() {
    if (server_thread_.joinable()) {
        server_.stop();
        server_thread_.join();
        if (listen_failed_.load()) {
            throw std::runtime_error("The API gateway listener stopped unexpectedly.");
        }
    }
}

bool ApiGateway::authorize(
    const httplib::Request& request,
    httplib::Response& response) {
    ++requests_total_;
    if (!allow_request()) {
        ++rejected_total_;
        set_json(response, 429, Json{{"error", "rate limit exceeded"}});
        return false;
    }
    if (!constant_time_equal(
            config_.api_key, request.get_header_value("X-API-Key"))) {
        ++rejected_total_;
        set_json(response, 401, Json{{"error", "unauthorized"}});
        return false;
    }
    return true;
}

bool ApiGateway::allow_request() {
    std::lock_guard<std::mutex> lock(rate_mutex_);
    const std::chrono::steady_clock::time_point now =
        std::chrono::steady_clock::now();
    if (now - rate_window_start_ >= std::chrono::minutes(1)) {
        rate_window_start_ = now;
        requests_in_window_ = 0;
    }
    if (requests_in_window_ >= config_.requests_per_minute) {
        return false;
    }
    ++requests_in_window_;
    return true;
}

void ApiGateway::register_routes() {
    server_.Get("/metrics", [this](const httplib::Request&,
                                   httplib::Response& response) {
        std::ostringstream metrics;
        metrics << "# HELP chronos_http_requests_total Authenticated API requests.\n"
                << "# TYPE chronos_http_requests_total counter\n"
                << "chronos_http_requests_total "
                << requests_total_.load() << '\n'
                << "# HELP chronos_http_rejected_total Rejected API requests.\n"
                << "# TYPE chronos_http_rejected_total counter\n"
                << "chronos_http_rejected_total "
                << rejected_total_.load() << '\n'
                << "# HELP chronos_jobs_submitted_total Accepted local jobs.\n"
                << "# TYPE chronos_jobs_submitted_total counter\n"
                << "chronos_jobs_submitted_total "
                << jobs_submitted_total_.load() << '\n'
                << "# HELP chronos_jobs_cancelled_total Cancelled jobs.\n"
                << "# TYPE chronos_jobs_cancelled_total counter\n"
                << "chronos_jobs_cancelled_total "
                << jobs_cancelled_total_.load() << '\n';
        response.set_content(metrics.str(), "text/plain; version=0.0.4");
    });

    server_.Post("/jobs", [this](const httplib::Request& request,
                                 httplib::Response& response) {
        if (!authorize(request, response)) {
            return;
        }

        Json body;
        try {
            body = Json::parse(request.body);
            if (!body.is_object()) {
                throw std::invalid_argument("The request body must be an object.");
            }

            Job job;
            job.id = body.at("id").get<std::string>();
            if (!valid_id(job.id)) {
                throw std::invalid_argument(
                    "id must contain 1-64 letters, digits, dots, underscores, or hyphens.");
            }

            const std::uint64_t priority = read_unsigned(
                body.at("priority"), 100, "priority");
            job.priority = static_cast<int>(priority);

            const Json& resources = body.at("resources");
            if (!resources.is_object()) {
                throw std::invalid_argument("resources must be an object.");
            }
            job.resources.cpu_millis = static_cast<std::uint32_t>(
                read_unsigned(resources.at("cpu_millis"),
                              std::numeric_limits<std::uint32_t>::max(),
                              "resources.cpu_millis"));
            job.resources.memory_bytes = read_unsigned(
                resources.at("memory_bytes"),
                std::numeric_limits<std::uint64_t>::max(),
                "resources.memory_bytes");
            job.resources.gpu_count = static_cast<std::uint32_t>(
                read_unsigned(resources.at("gpu_count"),
                              std::numeric_limits<std::uint32_t>::max(),
                              "resources.gpu_count"));

            const std::uint64_t duration_ms = body.contains("duration_ms")
                ? read_unsigned(body.at("duration_ms"),
                                config_.max_job_runtime_ms, "duration_ms")
                : 0;
            job.estimated_runtime_ms = body.contains("estimated_runtime_ms")
                ? read_unsigned(body.at("estimated_runtime_ms"),
                                config_.max_job_runtime_ms,
                                "estimated_runtime_ms")
                : duration_ms;
            if (body.contains("deadline_unix_ms")) {
                const std::uint64_t deadline_ms = read_unsigned(
                    body.at("deadline_unix_ms"),
                    static_cast<std::uint64_t>(
                        std::numeric_limits<std::int64_t>::max()),
                    "deadline_unix_ms");
                job.has_deadline = true;
                job.deadline = std::chrono::system_clock::time_point(
                    std::chrono::milliseconds(
                        static_cast<std::int64_t>(deadline_ms)));
            }

            const bool accepted = config_.submit_job
                ? config_.submit_job(job)
                : scheduler_.submit(job, [duration_ms]() {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(duration_ms));
                });
            if (!accepted) {
                set_json(response, 409, Json{{"error", "job ID already exists"}});
                return;
            }
            ++jobs_submitted_total_;
            const JobState submitted_state = config_.submit_job
                ? JobState::Submitted : JobState::Pending;
            log_event("job_submitted", job.id, to_string(submitted_state));
            set_json(response, 202, Json{
                {"id", job.id},
                {"state", to_string(submitted_state)}
            });
        } catch (const std::invalid_argument& error) {
            set_json(response, 400, Json{{"error", error.what()}});
        } catch (const Json::exception& error) {
            set_json(response, 400, Json{{"error", error.what()}});
        } catch (const std::exception& error) {
            std::clog << nlohmann::json{
                {"level", "error"},
                {"event", "job_submission_failed"},
                {"error", error.what()}
            }.dump() << '\n';
            set_json(response, 503, Json{{"error", "job backend unavailable"}});
        }
    });

    server_.Get(R"(/jobs/([A-Za-z0-9_.-]+))",
        [this](const httplib::Request& request, httplib::Response& response) {
            if (!authorize(request, response)) {
                return;
            }
            try {
                const std::string job_id = request.matches[1];
                const JobStatus status = config_.query_job
                    ? config_.query_job(job_id) : scheduler_.status(job_id);
                set_json(response, 200, status_json(status));
            } catch (const std::out_of_range&) {
                set_json(response, 404, Json{{"error", "job not found"}});
            } catch (const std::exception& error) {
                std::clog << nlohmann::json{
                    {"level", "error"},
                    {"event", "job_query_failed"},
                    {"job_id", request.matches[1].str()},
                    {"error", error.what()}
                }.dump() << '\n';
                set_json(response, 503, Json{{"error", "job backend unavailable"}});
            }
        });

    server_.Delete(R"(/jobs/([A-Za-z0-9_.-]+))",
        [this](const httplib::Request& request, httplib::Response& response) {
            if (!authorize(request, response)) {
                return;
            }
            const std::string job_id = request.matches[1];
            try {
                const bool cancelled = config_.cancel_job
                    ? config_.cancel_job(job_id) : scheduler_.cancel(job_id);
                if (!cancelled) {
                    set_json(response, 409,
                             Json{{"error", "job is no longer cancellable"}});
                    return;
                }
                ++jobs_cancelled_total_;
                const JobStatus status = config_.query_job
                    ? config_.query_job(job_id) : scheduler_.status(job_id);
                log_event("job_cancellation_requested", job_id,
                          to_string(status.state));
                set_json(response, 202,
                         status_json(status));
            } catch (const std::out_of_range&) {
                set_json(response, 404, Json{{"error", "job not found"}});
            } catch (const std::exception& error) {
                std::clog << nlohmann::json{
                    {"level", "error"},
                    {"event", "job_cancellation_failed"},
                    {"job_id", job_id},
                    {"error", error.what()}
                }.dump() << '\n';
                set_json(response, 503, Json{{"error", "job backend unavailable"}});
            }
        });
}

nlohmann::json ApiGateway::status_json(const JobStatus& status) {
    return nlohmann::json{
        {"id", status.job_id},
        {"state", to_string(status.state)},
        {"attempts", status.attempts},
        {"error", status.error}
    };
}

void ApiGateway::log_event(
    const std::string& event,
    const std::string& job_id,
    const std::string& state) {
    const std::int64_t timestamp_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    const Json entry{
        {"timestamp_unix_ms", timestamp_ms},
        {"level", "info"},
        {"event", event},
        {"job_id", job_id},
        {"state", state}
    };
    std::lock_guard<std::mutex> lock(log_mutex());
    std::clog << entry.dump() << '\n';
}

}
