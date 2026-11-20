#include "chronos/persistence/job_repository.hpp"

#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>

int main() {
    const char* connection_string = std::getenv("CHRONOS_DATABASE_URL");
    if (connection_string == NULL || *connection_string == '\0') {
        std::cerr << "CHRONOS_DATABASE_URL must be configured.\n";
        return 1;
    }
    const char* migrations_dir = std::getenv("CHRONOS_MIGRATIONS_DIR");
    const std::string migration = std::string(
        migrations_dir == NULL ? "sql" : migrations_dir) +
        "/001_initial_schema.sql";

    try {
        chronos::JobRepository repository(connection_string);
        repository.apply_migration(migration);
        chronos::Job job;
        job.id = "postgres-demo-job";
        job.priority = 10;
        job.resources.cpu_millis = 250;
        job.resources.memory_bytes = 1024 * 1024;
        if (!repository.create_job(job)) {
            std::cout << "Job already exists; PostgreSQL insert was idempotent.\n";
        } else {
            std::cout << "Persisted job " << job.id << ".\n";
        }
        chronos::PersistentJob stored;
        if (!repository.get_job(job.id, stored)) {
            throw std::runtime_error("Persisted demo job could not be queried.");
        }
        std::cout << stored.job.id << ": " << chronos::to_string(stored.state)
                  << ", version " << stored.version << '\n';
    } catch (const std::exception& error) {
        std::cerr << "PostgreSQL demo failed: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
