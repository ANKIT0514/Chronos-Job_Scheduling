#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace chronos {

struct ResourceRequest {
    std::uint32_t cpu_millis;
    std::uint64_t memory_bytes;
    std::uint32_t gpu_count;

    ResourceRequest()
        : cpu_millis(0), memory_bytes(0), gpu_count(0) {}
};

struct Job {
    std::string id;
    int priority;
    ResourceRequest resources;
    bool has_deadline;
    std::chrono::system_clock::time_point deadline;
    std::uint64_t estimated_runtime_ms;

    Job()
        : priority(0),
          has_deadline(false),
          deadline(std::chrono::system_clock::time_point::max()),
          estimated_runtime_ms(0) {}
};

}
