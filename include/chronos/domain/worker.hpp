#pragma once

#include "chronos/domain/job.hpp"

#include <cstdint>
#include <string>

namespace chronos {

struct Worker {
    std::string id;
    ResourceRequest capacity;
    ResourceRequest allocated;
    bool healthy;
    std::uint64_t estimated_start_delay_ms;

    Worker() : healthy(true), estimated_start_delay_ms(0) {}
};

}
