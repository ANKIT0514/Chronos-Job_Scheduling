#include "chronos/api/api_gateway.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>

#ifdef CHRONOS_DISTRIBUTED_BACKEND
#include "chronos/persistence/job_repository.hpp"
#include "chronos/streaming/kafka_event_bus.hpp"
#include <nlohmann/json.hpp>
#endif

namespace {

int read_port(const char* value) {
    if (value == NULL || *value == '\0') {
        return 8080;
    }
    std::size_t parsed = 0;
    const unsigned long port = std::stoul(value, &parsed);
    if (value[parsed] != '\0' || port == 0 ||
        port > std::numeric_limits<unsigned short>::max()) {
        throw std::invalid_argument("CHRONOS_API_PORT must be between 1 and 65535.");
    }
    return static_cast<int>(port);
}

}

int main() {
    const char* api_key = std::getenv("CHRONOS_API_KEY");
    if (api_key == NULL || *api_key == '\0') {
        std::cerr << "CHRONOS_API_KEY must be configured.\n";
        return 1;
    }

    const char* configured_host = std::getenv("CHRONOS_API_HOST");
    const std::string host = configured_host == NULL
        ? "0.0.0.0" : configured_host;

    try {
        chronos::InMemoryScheduler scheduler(4);
        chronos::ApiConfig config;
        config.api_key = api_key;
#ifdef CHRONOS_DISTRIBUTED_BACKEND
        const char* brokers = std::getenv("CHRONOS_KAFKA_BROKERS");
        const char* database_url = std::getenv("CHRONOS_DATABASE_URL");
        if (brokers == NULL || *brokers == '\0' ||
            database_url == NULL || *database_url == '\0') {
            throw std::invalid_argument(
                "CHRONOS_KAFKA_BROKERS and CHRONOS_DATABASE_URL must be configured.");
        }
        chronos::KafkaEventProducer producer(brokers);
        chronos::JobRepository repository(database_url);
        const char* migrations_dir = std::getenv("CHRONOS_MIGRATIONS_DIR");
        const std::string migration = std::string(
            migrations_dir == NULL ? "sql" : migrations_dir) +
            "/001_initial_schema.sql";
        repository.apply_migration(migration);
        std::atomic<std::uint64_t> event_sequence(0);

        config.submit_job = [&producer, &repository](
            const chronos::Job& job) {
            if (!repository.create_job(job)) {
                return false;
            }
            const nlohmann::json event{
                {"event_id", job.id + "-submitted"},
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
            producer.publish(
                chronos::kafka_topics::job_submission,
                job.id,
                event.dump());
            return true;
        };
        config.query_job = [&repository](
            const std::string& job_id) {
            chronos::PersistentJob stored;
            if (!repository.get_job(job_id, stored)) {
                throw std::out_of_range("Unknown job ID: " + job_id);
            }
            return chronos::JobStatus{
                job_id, stored.state, stored.error, stored.attempts};
        };
        config.cancel_job = [&producer, &repository, &event_sequence](
            const std::string& job_id) {
            chronos::PersistentJob stored;
            if (!repository.get_job(job_id, stored)) {
                throw std::out_of_range("Unknown job ID: " + job_id);
            }
            if (stored.state != chronos::JobState::Submitted &&
                stored.state != chronos::JobState::Pending &&
                stored.state != chronos::JobState::Scheduled &&
                stored.state != chronos::JobState::Running) {
                return false;
            }
            const std::string event_id = job_id + "-cancel-" +
                std::to_string(++event_sequence);
            producer.publish(
                chronos::kafka_topics::job_cancellation, job_id,
                nlohmann::json{
                    {"event_id", event_id},
                    {"id", job_id}
                }.dump());
            return true;
        };
#endif
        chronos::ApiGateway gateway(scheduler, config);
        gateway.listen(host, read_port(std::getenv("CHRONOS_API_PORT")));
    } catch (const std::exception& error) {
        std::cerr << "API gateway failed: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
