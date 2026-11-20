#include "chronos/version.hpp"
#include "chronos/scheduler/in_memory_scheduler.hpp"

#include <atomic>
#include <iostream>

int main() {
    std::cout << "Chronos " << chronos::version << " is ready.\n";
    chronos::InMemoryScheduler scheduler(2);
    std::atomic<int> completed(0);

    for (int i = 1; i <= 4; ++i) {
        chronos::Job job;
        job.id = "demo-" + std::to_string(i);
        job.priority = i;
        scheduler.submit(job, [&completed]() { ++completed; });
    }

    for (int i = 1; i <= 4; ++i) {
        const chronos::JobStatus status =
            scheduler.wait("demo-" + std::to_string(i));
        std::cout << status.job_id << ": " << chronos::to_string(status.state)
                  << '\n';
    }
    std::cout << "Completed " << completed.load() << " demo jobs.\n";
    return 0;
}
