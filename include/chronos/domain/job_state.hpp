#pragma once

namespace chronos {

enum class JobState {
    Submitted,
    Pending,
    Scheduled,
    Running,
    Succeeded,
    Failed,
    Retrying,
    DeadLetter,
    Cancelled
};

const char* to_string(JobState state);

}
