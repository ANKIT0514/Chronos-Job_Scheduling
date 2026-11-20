#include "chronos/services/service_utils.hpp"
#include "chronos/streaming/kafka_event_bus.hpp"

#include <nlohmann/json.hpp>

#include <atomic>
#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace {

std::atomic<bool> running(true);

void stop_service(int) {
    running.store(false);
}

}

int main() {
    try {
        const std::string brokers =
            chronos::service_utils::environment("CHRONOS_KAFKA_BROKERS");
        const std::string worker_id =
            chronos::service_utils::environment(
                "CHRONOS_WORKER_ID",
                chronos::service_utils::environment("HOSTNAME", "worker-1"));
        const std::string consumer_group = "chronos-worker-" + worker_id;
        const std::uint32_t cpu = static_cast<std::uint32_t>(std::stoul(
            chronos::service_utils::environment(
                "CHRONOS_WORKER_CPU_MILLIS", "4000")));
        const std::uint64_t memory = std::stoull(
            chronos::service_utils::environment(
                "CHRONOS_WORKER_MEMORY_BYTES", "8589934592"));
        const std::uint32_t gpu = static_cast<std::uint32_t>(std::stoul(
            chronos::service_utils::environment(
                "CHRONOS_WORKER_GPU_COUNT", "0")));
        const std::uint64_t heartbeat_interval_ms = std::stoull(
            chronos::service_utils::environment(
                "CHRONOS_HEARTBEAT_INTERVAL_MS", "2000"));
        const std::uint64_t fail_first_jobs = std::stoull(
            chronos::service_utils::environment(
                "CHRONOS_FAIL_FIRST_JOBS", "0"));

        chronos::KafkaEventProducer producer(brokers);
        chronos::KafkaEventConsumer consumer(
            brokers, consumer_group,
            std::vector<std::string>{chronos::kafka_topics::job_assignments});
        std::signal(SIGINT, stop_service);
        std::signal(SIGTERM, stop_service);

        std::uint64_t sequence = 0;
        std::set<std::string> injected_failures;
        const auto next_heartbeat =
            [&]() { return std::chrono::steady_clock::now(); };
        std::chrono::steady_clock::time_point heartbeat_due =
            next_heartbeat();
        while (running.load()) {
            const std::chrono::steady_clock::time_point now =
                std::chrono::steady_clock::now();
            if (now >= heartbeat_due) {
                const nlohmann::json heartbeat{
                    {"event_id", worker_id + "-heartbeat-" +
                                     std::to_string(++sequence)},
                    {"worker_id", worker_id},
                    {"cpu_capacity_millis", cpu},
                    {"memory_capacity_bytes", memory},
                    {"gpu_capacity_count", gpu},
                    {"healthy", true},
                    {"estimated_start_delay_ms", 0}
                };
                producer.publish(
                    chronos::kafka_topics::worker_heartbeats,
                    worker_id, heartbeat.dump());
                heartbeat_due = now + std::chrono::milliseconds(
                    heartbeat_interval_ms);
            }

            chronos::KafkaEvent event;
            if (!consumer.poll(event, 200)) {
                continue;
            }
            const nlohmann::json assignment =
                chronos::service_utils::parse_event(event.payload);
            const chronos::Job job =
                chronos::service_utils::read_job(assignment);
            const std::string event_id =
                assignment.at("event_id").get<std::string>();
            const std::uint64_t attempt =
                assignment.value("attempt", static_cast<std::uint64_t>(1));
            const nlohmann::json started{
                {"event_id", event_id + "-running"},
                {"job_id", job.id},
                {"worker_id", worker_id},
                {"state", "RUNNING"},
                {"attempt", attempt}
            };
            producer.publish(
                chronos::kafka_topics::worker_events, job.id, started.dump());
            const bool should_fail =
                attempt == 1 && fail_first_jobs > injected_failures.size() &&
                injected_failures.insert(job.id).second;
            std::this_thread::sleep_for(std::chrono::milliseconds(
                job.estimated_runtime_ms == 0 ? 50 :
                    std::min<std::uint64_t>(
                        job.estimated_runtime_ms, 60000)));
            const nlohmann::json finished{
                {"event_id", event_id + (should_fail ? "-failed" : "-succeeded")},
                {"job_id", job.id},
                {"worker_id", worker_id},
                {"state", should_fail ? "FAILED" : "SUCCEEDED"},
                {"attempt", attempt},
                {"error", should_fail ? "injected worker failure" : ""}
            };
            producer.publish(
                chronos::kafka_topics::worker_events, job.id, finished.dump());
            consumer.acknowledge();
        }
        producer.flush();
    } catch (const std::exception& error) {
        std::cerr << "Worker agent failed: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
