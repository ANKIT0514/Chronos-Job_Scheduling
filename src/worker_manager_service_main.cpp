#include "chronos/persistence/redis_worker_store.hpp"
#include "chronos/services/service_utils.hpp"
#include "chronos/streaming/kafka_event_bus.hpp"

#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
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
        const std::string redis_host =
            chronos::service_utils::environment(
                "CHRONOS_REDIS_HOST", "redis");
        const int redis_port = std::stoi(
            chronos::service_utils::environment(
                "CHRONOS_REDIS_PORT", "6379"));
        const char* configured_password =
            std::getenv("CHRONOS_REDIS_PASSWORD");
        const std::string password = configured_password == NULL
            ? std::string() : configured_password;
        const std::chrono::milliseconds ttl(
            std::stoll(chronos::service_utils::environment(
                "CHRONOS_WORKER_HEARTBEAT_TTL_MS", "10000")));

        chronos::KafkaEventConsumer consumer(
            brokers, "chronos-worker-manager",
            std::vector<std::string>{
                chronos::kafka_topics::worker_heartbeats,
                chronos::kafka_topics::job_assignments,
                chronos::kafka_topics::worker_events
            });
        chronos::RedisWorkerStore redis(redis_host, redis_port, password);
        std::signal(SIGINT, stop_service);
        std::signal(SIGTERM, stop_service);

        while (running.load()) {
            chronos::KafkaEvent event;
            if (!consumer.poll(event, 1000)) {
                continue;
            }
            const nlohmann::json body =
                chronos::service_utils::parse_event(event.payload);

            if (event.topic == chronos::kafka_topics::worker_heartbeats) {
                const chronos::Worker worker =
                    chronos::service_utils::read_worker(body);
                redis.store_heartbeat(worker, ttl);
                redis.store_load(worker, ttl);
            } else if (event.topic == chronos::kafka_topics::job_assignments) {
                const std::string worker_id =
                    body.at("worker_id").get<std::string>();
                const std::string job_id =
                    body.at("id").get<std::string>();
                redis.add_job(worker_id, job_id, ttl);
            } else if (event.topic == chronos::kafka_topics::worker_events) {
                const std::string worker_id =
                    body.at("worker_id").get<std::string>();
                const std::string job_id =
                    body.at("job_id").get<std::string>();
                const std::string state =
                    body.at("state").get<std::string>();
                if (state == "SUCCEEDED" || state == "FAILED") {
                    redis.remove_job(worker_id, job_id);
                }
            }
            consumer.acknowledge();
        }
    } catch (const std::exception& error) {
        std::cerr << "Worker manager service failed: "
                  << error.what() << '\n';
        return 1;
    }
    return 0;
}
