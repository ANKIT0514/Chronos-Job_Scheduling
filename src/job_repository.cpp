#include "chronos/persistence/job_repository.hpp"

#include <pqxx/pqxx>

#include <cstdint>
#include <fstream>
#include <iterator>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>

namespace chronos {
namespace {

std::string numeric_string(std::uint64_t value) {
    return std::to_string(value);
}

std::int64_t unix_milliseconds(
    std::chrono::system_clock::time_point time) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        time.time_since_epoch()).count();
}

JobState parse_state(const std::string& state) {
    if (state == "SUBMITTED") return JobState::Submitted;
    if (state == "PENDING") return JobState::Pending;
    if (state == "SCHEDULED") return JobState::Scheduled;
    if (state == "RUNNING") return JobState::Running;
    if (state == "SUCCEEDED") return JobState::Succeeded;
    if (state == "FAILED") return JobState::Failed;
    if (state == "RETRYING") return JobState::Retrying;
    if (state == "DEAD_LETTER") return JobState::DeadLetter;
    if (state == "CANCELLED") return JobState::Cancelled;
    throw std::runtime_error("Unknown persisted job state: " + state);
}

bool valid_transition(JobState from, JobState to) {
    switch (from) {
        case JobState::Submitted:
            return to == JobState::Pending || to == JobState::Cancelled;
        case JobState::Pending:
            return to == JobState::Scheduled || to == JobState::Cancelled;
        case JobState::Scheduled:
            return to == JobState::Running || to == JobState::Pending ||
                   to == JobState::Cancelled;
        case JobState::Running:
            return to == JobState::Succeeded || to == JobState::Failed ||
                   to == JobState::Retrying || to == JobState::Cancelled;
        case JobState::Failed:
            return to == JobState::Retrying || to == JobState::DeadLetter;
        case JobState::Retrying:
            return to == JobState::Pending || to == JobState::DeadLetter;
        case JobState::Succeeded:
        case JobState::DeadLetter:
        case JobState::Cancelled:
            return false;
    }
    return false;
}

}

JobRepository::JobRepository(const std::string& connection_string)
    : connection_string_(connection_string) {
    if (connection_string_.empty()) {
        throw std::invalid_argument("The PostgreSQL connection string is empty.");
    }
}

void JobRepository::apply_migration(
    const std::string& migration_file) const {
    std::ifstream input(migration_file.c_str(), std::ios::in | std::ios::binary);
    if (!input) {
        throw std::runtime_error(
            "Could not open PostgreSQL migration: " + migration_file);
    }
    const std::string migration(
        (std::istreambuf_iterator<char>(input)),
        std::istreambuf_iterator<char>());
    if (migration.empty()) {
        throw std::runtime_error(
            "PostgreSQL migration is empty: " + migration_file);
    }

    pqxx::connection connection(connection_string_);
    pqxx::work transaction(connection);
    transaction.exec(migration);
    transaction.commit();
}

bool JobRepository::create_job(const Job& job) const {
    if (job.id.empty()) {
        throw std::invalid_argument("A persisted job must have a non-empty ID.");
    }
    if (job.priority < 0 || job.priority > 100) {
        throw std::invalid_argument("Job priority must be between 0 and 100.");
    }

    pqxx::connection connection(connection_string_);
    pqxx::work transaction(connection);
    const std::string deadline_ms = job.has_deadline
        ? std::to_string(unix_milliseconds(job.deadline)) : "0";
    const pqxx::result inserted = transaction.exec_params(
        "INSERT INTO jobs "
        "(id, priority, cpu_millis, memory_bytes, gpu_count, "
        " deadline_unix_ms, state, version) "
        "VALUES ($1, $2, $3, $4::numeric, $5, "
        " CASE WHEN $6 THEN $7::bigint ELSE NULL END, 'SUBMITTED', 0) "
        "ON CONFLICT (id) DO NOTHING",
        job.id, job.priority, job.resources.cpu_millis,
        numeric_string(job.resources.memory_bytes), job.resources.gpu_count,
        job.has_deadline, deadline_ms);
    if (inserted.affected_rows() == 0) {
        transaction.commit();
        return false;
    }
    transaction.exec_params(
        "INSERT INTO audit_logs (job_id, action, actor, details) "
        "VALUES ($1, 'SUBMITTED', 'api-gateway', '{}'::jsonb)",
        job.id);
    transaction.commit();
    return true;
}

TransitionResult JobRepository::transition_job(
    const std::string& job_id,
    const std::string& event_id,
    std::uint64_t expected_version,
    JobState new_state,
    const std::string& error,
    const std::string& worker_id) const {
    if (event_id.empty()) {
        throw std::invalid_argument("A transition event must have an ID.");
    }
    if (expected_version >
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
        throw std::invalid_argument("Job version exceeds PostgreSQL BIGINT.");
    }

    pqxx::connection connection(connection_string_);
    pqxx::work transaction(connection);
    const pqxx::result current = transaction.exec_params(
        "SELECT state, version FROM jobs WHERE id = $1 FOR UPDATE", job_id);
    if (current.empty()) {
        transaction.commit();
        return TransitionResult::Conflict;
    }

    const std::string previous_state =
        current[0]["state"].as<std::string>();
    const std::int64_t current_version =
        current[0]["version"].as<std::int64_t>();
    const pqxx::result existing = transaction.exec_params(
        "SELECT job_id, new_state, version FROM job_events WHERE event_id = $1",
        event_id);
    if (!existing.empty()) {
        const bool same_event =
            existing[0]["job_id"].as<std::string>() == job_id &&
            existing[0]["new_state"].as<std::string>() == to_string(new_state) &&
            existing[0]["version"].as<std::int64_t>() ==
                static_cast<std::int64_t>(expected_version + 1);
        transaction.commit();
        return same_event ? TransitionResult::Duplicate
                          : TransitionResult::IdempotencyConflict;
    }
    if (current_version != static_cast<std::int64_t>(expected_version)) {
        transaction.commit();
        return TransitionResult::Conflict;
    }
    if (!valid_transition(parse_state(previous_state), new_state)) {
        transaction.commit();
        return TransitionResult::InvalidTransition;
    }

    const std::string new_state_name = to_string(new_state);
    const std::int64_t new_version = current_version + 1;
    const pqxx::result updated = transaction.exec_params(
        "UPDATE jobs SET state = $2, version = $3, error = $4, "
        "updated_at = now() WHERE id = $1 AND version = $5",
        job_id, new_state_name, new_version, error, current_version);
    if (updated.affected_rows() != 1) {
        transaction.commit();
        return TransitionResult::Conflict;
    }
    transaction.exec_params(
        "INSERT INTO job_events "
        "(event_id, job_id, previous_state, new_state, version, error) "
        "VALUES ($1, $2, $3, $4, $5, $6)",
        event_id, job_id, previous_state, new_state_name, new_version,
        error);
    transaction.exec_params(
        "INSERT INTO audit_logs (job_id, action, actor, details) "
        "VALUES ($1, $2, $3, jsonb_build_object('event_id', $4::text))",
        job_id, new_state_name, worker_id.empty() ? "scheduler" : worker_id,
        event_id);

    if (new_state == JobState::Running) {
        transaction.exec_params(
            "INSERT INTO job_attempts (job_id, attempt_number, worker_id, state) "
            "SELECT $1, COALESCE(MAX(attempt_number), 0) + 1, "
            "NULLIF($2, ''), 'RUNNING' FROM job_attempts WHERE job_id = $1",
            job_id, worker_id);
    } else if (new_state == JobState::Succeeded ||
               new_state == JobState::Failed ||
               new_state == JobState::DeadLetter ||
               new_state == JobState::Cancelled) {
        transaction.exec_params(
            "UPDATE job_attempts SET state = $2, finished_at = now(), "
            "error = $3 WHERE job_id = $1 AND attempt_number = "
            "(SELECT MAX(attempt_number) FROM job_attempts WHERE job_id = $1)",
            job_id, new_state_name, error);
    }
    transaction.commit();
    return TransitionResult::Applied;
}

bool JobRepository::get_job(
    const std::string& job_id,
    PersistentJob& result) const {
    pqxx::connection connection(connection_string_);
    pqxx::read_transaction transaction(connection);
    const pqxx::result rows = transaction.exec_params(
        "SELECT id, priority, cpu_millis, memory_bytes::text, gpu_count, "
        "deadline_unix_ms, state, version, COALESCE(error, ''), "
        "(SELECT COUNT(*) FROM job_attempts WHERE job_id = jobs.id) "
        "FROM jobs WHERE id = $1",
        job_id);
    if (rows.empty()) {
        return false;
    }

    const pqxx::row& row = rows[0];
    result.job.id = row[0].as<std::string>();
    result.job.priority = row[1].as<int>();
    result.job.resources.cpu_millis = row[2].as<std::uint32_t>();
    result.job.resources.memory_bytes = static_cast<std::uint64_t>(
        std::stoull(row[3].as<std::string>()));
    result.job.resources.gpu_count = row[4].as<std::uint32_t>();
    result.job.has_deadline = !row[5].is_null();
    if (result.job.has_deadline) {
        result.job.deadline = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(row[5].as<std::int64_t>()));
    }
    result.state = parse_state(row[6].as<std::string>());
    result.version = row[7].as<std::uint64_t>();
    result.error = row[8].as<std::string>();
    result.attempts = row[9].as<std::size_t>();
    return true;
}

void JobRepository::upsert_worker(
    const Worker& worker,
    std::chrono::system_clock::time_point last_heartbeat) const {
    if (worker.id.empty()) {
        throw std::invalid_argument("A persisted worker must have a non-empty ID.");
    }

    pqxx::connection connection(connection_string_);
    pqxx::work transaction(connection);
    transaction.exec_params(
        "INSERT INTO workers "
        "(id, cpu_capacity, memory_capacity, gpu_capacity, cpu_allocated, "
        "memory_allocated, gpu_allocated, healthy, last_heartbeat) "
        "VALUES ($1, $2, $3::numeric, $4, $5, $6::numeric, $7, $8, "
        "to_timestamp($9::double precision / 1000.0)) "
        "ON CONFLICT (id) DO UPDATE SET "
        "cpu_capacity = EXCLUDED.cpu_capacity, "
        "memory_capacity = EXCLUDED.memory_capacity, "
        "gpu_capacity = EXCLUDED.gpu_capacity, "
        "cpu_allocated = EXCLUDED.cpu_allocated, "
        "memory_allocated = EXCLUDED.memory_allocated, "
        "gpu_allocated = EXCLUDED.gpu_allocated, "
        "healthy = EXCLUDED.healthy, "
        "last_heartbeat = EXCLUDED.last_heartbeat, updated_at = now()",
        worker.id, worker.capacity.cpu_millis,
        numeric_string(worker.capacity.memory_bytes), worker.capacity.gpu_count,
        worker.allocated.cpu_millis,
        numeric_string(worker.allocated.memory_bytes),
        worker.allocated.gpu_count, worker.healthy,
        unix_milliseconds(last_heartbeat));
    transaction.commit();
}

}
