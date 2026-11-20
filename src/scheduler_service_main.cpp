#include "chronos/domain/job_state.hpp"
#include "chronos/scheduler/retry_policy.hpp"
#include "chronos/scheduler/worker_selector.hpp"
#include "chronos/services/service_utils.hpp"
#include "chronos/streaming/kafka_event_bus.hpp"
#include "chronos/worker/worker_manager.hpp"

#include "chronos/persistence/job_repository.hpp"

#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

std::atomic<bool> running(true);

void stop_service(int) {
    running.store(false);
}

void log_event(
    const std::string& event,
    const std::string& entity_id,
    const std::string& detail) {
    std::clog << nlohmann::json{
        {"timestamp_unix_ms",
         std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch()).count()},
        {"level", "info"},
        {"event", event},
        {"entity_id", entity_id},
        {"detail", detail}
    }.dump() << '\n';
}

}

int main() {
    try {
        const std::string brokers =
            chronos::service_utils::environment("CHRONOS_KAFKA_BROKERS");
        const std::string database_url =
            chronos::service_utils::environment("CHRONOS_DATABASE_URL");
        const std::uint64_t heartbeat_timeout_ms = std::stoull(
            chronos::service_utils::environment(
                "CHRONOS_WORKER_HEARTBEAT_TIMEOUT_MS", "10000"));

        chronos::KafkaEventProducer producer(brokers);
        chronos::KafkaEventConsumer consumer(
            brokers, "chronos-scheduler",
            std::vector<std::string>{
                chronos::kafka_topics::job_submission,
                chronos::kafka_topics::job_cancellation,
                chronos::kafka_topics::worker_heartbeats,
                chronos::kafka_topics::worker_events
            });
        chronos::JobRepository repository(database_url);
        chronos::WorkerManager worker_manager(
            std::chrono::milliseconds(heartbeat_timeout_ms));
        const chronos::RetryPolicy retry_policy(
            3, std::chrono::milliseconds(100),
            std::chrono::milliseconds(2000));

        std::map<std::string, chronos::Job> pending_jobs;
        std::multimap<std::chrono::steady_clock::time_point, chronos::Job>
            delayed_retries;
        std::map<std::string, std::string> job_owners;
        std::map<std::string, std::size_t> attempts;
        std::uint64_t event_sequence = 0;
        std::signal(SIGINT, stop_service);
        std::signal(SIGTERM, stop_service);

        const auto persist_transition =
            [&](const std::string& job_id, chronos::JobState new_state,
                const std::string& error, const std::string& worker_id) {
                chronos::PersistentJob stored;
                if (!repository.get_job(job_id, stored)) {
                    throw std::runtime_error(
                        "Job metadata is missing: " + job_id);
                }
                const chronos::TransitionResult result =
                    repository.transition_job(
                        job_id,
                        job_id + "-scheduler-" +
                            std::to_string(++event_sequence),
                        stored.version, new_state, error, worker_id);
                if (result == chronos::TransitionResult::Applied ||
                    result == chronos::TransitionResult::Duplicate) {
                    return true;
                }
                if (result == chronos::TransitionResult::Conflict) {
                    chronos::PersistentJob current;
                    if (repository.get_job(job_id, current) &&
                        current.state == new_state) {
                        return true;
                    }
                }
                log_event("state_transition_rejected", job_id,
                          chronos::to_string(new_state));
                return false;
            };

        const auto publish_status =
            [&](const chronos::Job& job, chronos::JobState state,
                const std::string& error) {
                const chronos::PersistentJob stored =
                    [&]() {
                        chronos::PersistentJob value;
                        if (!repository.get_job(job.id, value)) {
                            throw std::runtime_error(
                                "Job disappeared while publishing status.");
                        }
                        return value;
                    }();
                const nlohmann::json status{
                    {"event_id", job.id + "-status-" +
                                     std::to_string(++event_sequence)},
                    {"id", job.id},
                    {"state", chronos::to_string(state)},
                    {"attempts", stored.attempts},
                    {"error", error}
                };
                producer.publish(
                    chronos::kafka_topics::job_status_updates,
                    job.id, status.dump());
            };

        const auto assign_pending = [&]() {
            std::vector<std::string> ready_ids;
            for (std::map<std::string, chronos::Job>::const_iterator job =
                     pending_jobs.begin();
                 job != pending_jobs.end(); ++job) {
                ready_ids.push_back(job->first);
            }
            for (std::vector<std::string>::const_iterator id =
                     ready_ids.begin();
                 id != ready_ids.end(); ++id) {
                const chronos::Job job = pending_jobs.find(*id)->second;
                chronos::WorkerScore selected;
                try {
                    selected = chronos::select_worker(
                        job, worker_manager.healthy_workers(),
                        chronos::SchedulingStrategy::ResourceAware);
                } catch (const std::runtime_error&) {
                    continue;
                }
                if (!worker_manager.assign_job(selected.worker_id, job)) {
                    continue;
                }
                const std::size_t attempt = ++attempts[job.id];
                job_owners[job.id] = selected.worker_id;
                const nlohmann::json assignment =
                    chronos::service_utils::job_event(
                        job, job.id + "-assignment-" +
                                 std::to_string(attempt));
                nlohmann::json mutable_assignment = assignment;
                mutable_assignment["attempt"] = attempt;
                mutable_assignment["worker_id"] = selected.worker_id;
                producer.publish(
                    chronos::kafka_topics::job_assignments,
                    job.id, mutable_assignment.dump());
                persist_transition(
                    job.id, chronos::JobState::Scheduled, std::string(),
                    selected.worker_id);
                publish_status(job, chronos::JobState::Scheduled,
                               std::string());
                pending_jobs.erase(*id);
                log_event("job_assigned", job.id, selected.worker_id);
            }
        };

        const auto retry_or_dead_letter =
            [&](const chronos::Job& job, const std::string& error,
                const std::string& worker_id) {
                chronos::PersistentJob stored;
                if (!repository.get_job(job.id, stored)) {
                    throw std::runtime_error("Job metadata disappeared.");
                }
                if (stored.state == chronos::JobState::Running) {
                    persist_transition(
                        job.id, chronos::JobState::Failed, error, worker_id);
                } else if (stored.state == chronos::JobState::Scheduled) {
                    persist_transition(
                        job.id, chronos::JobState::Pending, std::string(),
                        worker_id);
                }

                if (retry_policy.should_retry(attempts[job.id])) {
                    chronos::PersistentJob current;
                    if (!repository.get_job(job.id, current)) {
                        throw std::runtime_error("Job metadata disappeared.");
                    }
                    if (current.state == chronos::JobState::Failed) {
                        persist_transition(
                            job.id, chronos::JobState::Retrying, error, worker_id);
                        persist_transition(
                            job.id, chronos::JobState::Pending, std::string(),
                            worker_id);
                    }
                    delayed_retries.insert(std::make_pair(
                        std::chrono::steady_clock::now() +
                            retry_policy.delay_after_attempt(attempts[job.id]),
                        job));
                    publish_status(
                        job,
                        stored.state == chronos::JobState::Scheduled
                            ? chronos::JobState::Pending
                            : chronos::JobState::Retrying,
                        error);
                    return;
                }

                chronos::PersistentJob current;
                if (repository.get_job(job.id, current) &&
                    current.state == chronos::JobState::Failed) {
                    persist_transition(
                        job.id, chronos::JobState::DeadLetter, error, worker_id);
                }
                const nlohmann::json dead_letter{
                    {"event_id", job.id + "-dead-letter"},
                    {"id", job.id},
                    {"error", error},
                    {"attempts", attempts[job.id]}
                };
                producer.publish(
                    "job-dead-letter", job.id, dead_letter.dump());
                publish_status(job, chronos::JobState::DeadLetter, error);
            };

        while (running.load()) {
            const std::chrono::steady_clock::time_point now =
                std::chrono::steady_clock::now();
            const std::vector<chronos::WorkerFailure> failures =
                worker_manager.detect_timeouts(now);
            for (std::vector<chronos::WorkerFailure>::const_iterator failure =
                     failures.begin();
                 failure != failures.end(); ++failure) {
                for (std::vector<chronos::Job>::const_iterator job =
                         failure->lost_jobs.begin();
                     job != failure->lost_jobs.end(); ++job) {
                    job_owners.erase(job->id);
                    retry_or_dead_letter(
                        *job, "worker heartbeat timed out",
                        failure->worker_id);
                    log_event("worker_job_lost", job->id, failure->worker_id);
                }
            }

            while (!delayed_retries.empty() &&
                   delayed_retries.begin()->first <= now) {
                pending_jobs[delayed_retries.begin()->second.id] =
                    delayed_retries.begin()->second;
                delayed_retries.erase(delayed_retries.begin());
            }

            chronos::KafkaEvent event;
            if (!consumer.poll(event, 500)) {
                assign_pending();
                continue;
            }
            const nlohmann::json body =
                chronos::service_utils::parse_event(event.payload);

            if (event.topic == chronos::kafka_topics::worker_heartbeats) {
                const chronos::Worker worker =
                    chronos::service_utils::read_worker(body);
                if (!worker_manager.register_worker(
                        worker, std::chrono::steady_clock::now())) {
                    worker_manager.heartbeat(
                        worker.id, std::chrono::steady_clock::now());
                }
                consumer.acknowledge();
                assign_pending();
                continue;
            }

            if (event.topic == chronos::kafka_topics::job_submission) {
                const chronos::Job job =
                    chronos::service_utils::read_job(body);
                if (pending_jobs.find(job.id) == pending_jobs.end() &&
                    job_owners.find(job.id) == job_owners.end() &&
                    attempts.find(job.id) == attempts.end()) {
                    chronos::PersistentJob stored;
                    if (!repository.get_job(job.id, stored)) {
                        throw std::runtime_error(
                            "Submission references missing metadata: " + job.id);
                    }
                    attempts[job.id] = stored.attempts;
                    if (stored.state == chronos::JobState::Submitted) {
                        persist_transition(
                            job.id, chronos::JobState::Pending,
                            std::string(), std::string());
                        pending_jobs.insert(std::make_pair(job.id, job));
                        publish_status(
                            job, chronos::JobState::Pending, std::string());
                    } else if (stored.state == chronos::JobState::Pending) {
                        pending_jobs.insert(std::make_pair(job.id, job));
                    }
                }
                consumer.acknowledge();
                assign_pending();
                continue;
            }

            if (event.topic == chronos::kafka_topics::job_cancellation) {
                const std::string job_id =
                    body.at("id").get<std::string>();
                chronos::PersistentJob stored;
                if (repository.get_job(job_id, stored) &&
                    (stored.state == chronos::JobState::Submitted ||
                     stored.state == chronos::JobState::Pending ||
                     stored.state == chronos::JobState::Scheduled ||
                     stored.state == chronos::JobState::Running)) {
                    pending_jobs.erase(job_id);
                    const std::map<std::string, std::string>::iterator owner =
                        job_owners.find(job_id);
                    if (owner != job_owners.end()) {
                        worker_manager.complete_job(job_id, owner->second);
                        job_owners.erase(owner);
                    }
                    persist_transition(
                        job_id, chronos::JobState::Cancelled,
                        "cancelled by client", std::string());
                    publish_status(
                        stored.job, chronos::JobState::Cancelled,
                        "cancelled by client");
                }
                consumer.acknowledge();
                continue;
            }

            if (event.topic == chronos::kafka_topics::worker_events) {
                const std::string job_id =
                    body.at("job_id").get<std::string>();
                const std::string worker_id =
                    body.at("worker_id").get<std::string>();
                const std::string state =
                    body.at("state").get<std::string>();
                const std::uint64_t attempt =
                    body.value("attempt", static_cast<std::uint64_t>(0));
                const std::map<std::string, std::string>::const_iterator owner =
                    job_owners.find(job_id);
                if (owner == job_owners.end() ||
                    owner->second != worker_id ||
                    attempts[job_id] != attempt) {
                    consumer.acknowledge();
                    log_event("stale_worker_event_ignored", job_id, worker_id);
                    continue;
                }
                chronos::PersistentJob stored;
                if (!repository.get_job(job_id, stored)) {
                    throw std::runtime_error(
                        "Worker event references an unknown job: " + job_id);
                }

                if (state == "RUNNING") {
                    persist_transition(
                        job_id, chronos::JobState::Running, std::string(),
                        worker_id);
                    publish_status(
                        stored.job, chronos::JobState::Running, std::string());
                } else if (state == "SUCCEEDED") {
                    worker_manager.complete_job(job_id, worker_id);
                    job_owners.erase(job_id);
                    persist_transition(
                        job_id, chronos::JobState::Succeeded, std::string(),
                        worker_id);
                    publish_status(
                        stored.job, chronos::JobState::Succeeded, std::string());
                } else if (state == "FAILED") {
                    worker_manager.complete_job(job_id, worker_id);
                    job_owners.erase(job_id);
                    retry_or_dead_letter(
                        stored.job, body.value("error", "worker failed"),
                        worker_id);
                } else {
                    throw std::invalid_argument(
                        "Worker event contains an unsupported state.");
                }
                consumer.acknowledge();
                assign_pending();
            }
        }
        producer.flush();
    } catch (const std::exception& error) {
        std::cerr << "Scheduler service failed: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
