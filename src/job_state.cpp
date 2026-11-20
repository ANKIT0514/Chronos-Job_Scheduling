#include "chronos/domain/job_state.hpp"

namespace chronos {

const char* to_string(JobState state) {
    switch (state) {
        case JobState::Submitted:
            return "SUBMITTED";
        case JobState::Pending:
            return "PENDING";
        case JobState::Scheduled:
            return "SCHEDULED";
        case JobState::Running:
            return "RUNNING";
        case JobState::Succeeded:
            return "SUCCEEDED";
        case JobState::Failed:
            return "FAILED";
        case JobState::Retrying:
            return "RETRYING";
        case JobState::DeadLetter:
            return "DEAD_LETTER";
        case JobState::Cancelled:
            return "CANCELLED";
    }
    return "UNKNOWN";
}

}
