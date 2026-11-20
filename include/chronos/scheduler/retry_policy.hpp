#pragma once

#include <chrono>
#include <cstddef>

namespace chronos {

class RetryPolicy {
public:
    explicit RetryPolicy(
        std::size_t max_attempts = 3,
        std::chrono::milliseconds initial_backoff =
            std::chrono::milliseconds(25),
        std::chrono::milliseconds max_backoff =
            std::chrono::milliseconds(1000));

    bool should_retry(std::size_t completed_attempts) const;
    std::chrono::milliseconds delay_after_attempt(
        std::size_t completed_attempts) const;
    std::size_t max_attempts() const;

private:
    std::size_t max_attempts_;
    std::chrono::milliseconds initial_backoff_;
    std::chrono::milliseconds max_backoff_;
};

}
