#pragma once

#include "chronos/domain/job.hpp"
#include "chronos/domain/job_state.hpp"
#include "chronos/domain/worker.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>

namespace chronos {

enum class TransitionResult {
    Applied,
    Duplicate,
    Conflict,
    InvalidTransition,
    IdempotencyConflict
};

struct PersistentJob {
    Job job;
    JobState state;
    std::uint64_t version;
    std::size_t attempts;
    std::string error;
};

class JobRepository {
public:
    explicit JobRepository(const std::string& connection_string);

    void apply_migration(const std::string& migration_file) const;
    bool create_job(const Job& job) const;
    TransitionResult transition_job(
        const std::string& job_id,
        const std::string& event_id,
        std::uint64_t expected_version,
        JobState new_state,
        const std::string& error = std::string(),
        const std::string& worker_id = std::string()) const;
    bool get_job(const std::string& job_id, PersistentJob& result) const;
    void upsert_worker(
        const Worker& worker,
        std::chrono::system_clock::time_point last_heartbeat =
            std::chrono::system_clock::now()) const;

private:
    const std::string connection_string_;
};

}
