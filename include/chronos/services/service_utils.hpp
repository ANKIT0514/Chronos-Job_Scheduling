#pragma once

#include "chronos/domain/job.hpp"
#include "chronos/domain/worker.hpp"

#include <nlohmann/json.hpp>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <string>

namespace chronos {
namespace service_utils {

inline nlohmann::json parse_event(const std::string& payload) {
    const nlohmann::json event = nlohmann::json::parse(payload);
    if (!event.is_object()) {
        throw std::invalid_argument("Kafka event must be a JSON object.");
    }
    return event;
}

inline Job read_job(const nlohmann::json& event) {
    Job job;
    job.id = event.at("id").get<std::string>();
    job.priority = event.at("priority").get<int>();
    job.resources.cpu_millis =
        event.at("cpu_millis").get<std::uint32_t>();
    job.resources.memory_bytes =
        event.at("memory_bytes").get<std::uint64_t>();
    job.resources.gpu_count =
        event.at("gpu_count").get<std::uint32_t>();
    job.estimated_runtime_ms =
        event.value("estimated_runtime_ms", static_cast<std::uint64_t>(0));
    job.has_deadline = event.value("has_deadline", false);
    if (job.has_deadline) {
        job.deadline = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(
                event.at("deadline_unix_ms").get<std::int64_t>()));
    }
    if (job.id.empty() || job.priority < 0 || job.priority > 100) {
        throw std::invalid_argument("Kafka job has invalid ID or priority.");
    }
    return job;
}

inline Worker read_worker(const nlohmann::json& event) {
    Worker worker;
    worker.id = event.at("worker_id").get<std::string>();
    worker.capacity.cpu_millis =
        event.at("cpu_capacity_millis").get<std::uint32_t>();
    worker.capacity.memory_bytes =
        event.at("memory_capacity_bytes").get<std::uint64_t>();
    worker.capacity.gpu_count =
        event.at("gpu_capacity_count").get<std::uint32_t>();
    worker.healthy = event.value("healthy", true);
    worker.estimated_start_delay_ms =
        event.value("estimated_start_delay_ms",
                    static_cast<std::uint64_t>(0));
    if (worker.id.empty()) {
        throw std::invalid_argument("Kafka worker heartbeat has no worker ID.");
    }
    return worker;
}

inline nlohmann::json job_event(
    const Job& job,
    const std::string& event_id) {
    return nlohmann::json{
        {"event_id", event_id},
        {"id", job.id},
        {"priority", job.priority},
        {"cpu_millis", job.resources.cpu_millis},
        {"memory_bytes", job.resources.memory_bytes},
        {"gpu_count", job.resources.gpu_count},
        {"estimated_runtime_ms", job.estimated_runtime_ms},
        {"has_deadline", job.has_deadline},
        {"deadline_unix_ms", job.has_deadline
            ? std::chrono::duration_cast<std::chrono::milliseconds>(
                  job.deadline.time_since_epoch()).count()
            : 0}
    };
}

inline std::string environment(
    const char* name,
    const std::string& fallback = std::string()) {
    const char* value = std::getenv(name);
    if (value == NULL || *value == '\0') {
        if (fallback.empty()) {
            throw std::invalid_argument(
                std::string("Required environment variable is missing: ") + name);
        }
        return fallback;
    }
    return value;
}

}
}
