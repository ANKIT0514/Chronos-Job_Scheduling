#include "chronos/scheduler/in_memory_scheduler.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <sstream>
#include <string>
#include <vector>

#ifdef _WIN32
#include <direct.h>
#include <windows.h>
#include <winreg.h>
#else
#include <sys/stat.h>
#include <sys/utsname.h>
#include <unistd.h>
#endif

namespace {

struct Options {
    std::size_t jobs;
    std::uint64_t duration_ms;
    std::string output_dir;

    Options()
        : jobs(1000), duration_ms(1), output_dir("docs/benchmarks") {}
};

struct Measurement {
    int workers;
    std::size_t jobs;
    std::uint64_t duration_ms;
    double throughput;
    double p50;
    double p95;
    double p99;
    double resource_utilization;
    double failure_recovery_ms;
    double elapsed_ms;
};

Options parse_options(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string argument(argv[i]);
        if (argument == "--jobs" && i + 1 < argc) {
            options.jobs = static_cast<std::size_t>(
                std::stoull(argv[++i]));
        } else if (argument == "--duration-ms" && i + 1 < argc) {
            options.duration_ms = std::stoull(argv[++i]);
        } else if (argument == "--output-dir" && i + 1 < argc) {
            options.output_dir = argv[++i];
        } else {
            throw std::invalid_argument(
                "Usage: chronos_benchmark [--jobs N] [--duration-ms N] "
                "[--output-dir PATH]");
        }
    }
    if (options.jobs == 0 || options.duration_ms == 0 ||
        options.duration_ms > 60000) {
        throw std::invalid_argument(
            "Job count and runtime must be positive; runtime must be <= 60000 ms.");
    }
    return options;
}

std::string machine_specs() {
#ifdef _WIN32
    SYSTEM_INFO system_info;
    GetSystemInfo(&system_info);
    MEMORYSTATUSEX memory_status;
    memory_status.dwLength = sizeof(memory_status);
    if (GlobalMemoryStatusEx(&memory_status) == 0) {
        throw std::runtime_error("Could not read Windows memory information.");
    }
    char cpu_identifier[512] = {0};
    const DWORD identifier_size = GetEnvironmentVariableA(
        "PROCESSOR_IDENTIFIER", cpu_identifier, sizeof(cpu_identifier));
    std::string cpu = identifier_size == 0 ||
                            identifier_size >= sizeof(cpu_identifier)
        ? "not reported by PROCESSOR_IDENTIFIER"
        : std::string(cpu_identifier);
    HKEY processor_key = NULL;
    if (RegOpenKeyExA(
            HKEY_LOCAL_MACHINE,
            "HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0",
            0, KEY_QUERY_VALUE, &processor_key) == ERROR_SUCCESS) {
        char processor_name[512] = {0};
        DWORD processor_name_size = sizeof(processor_name);
        DWORD value_type = 0;
        if (RegQueryValueExA(
                processor_key, "ProcessorNameString", NULL, &value_type,
                reinterpret_cast<LPBYTE>(processor_name),
                &processor_name_size) == ERROR_SUCCESS &&
            (value_type == REG_SZ || value_type == REG_EXPAND_SZ)) {
            cpu = processor_name;
        }
        RegCloseKey(processor_key);
    }
    std::ostringstream result;
    result << "{\n"
           << "  \"os\": \"Windows\",\n"
           << "  \"cpu_identifier\": \"" << cpu << "\",\n"
           << "  \"logical_processors\": "
           << system_info.dwNumberOfProcessors << ",\n"
           << "  \"physical_memory_bytes\": "
           << memory_status.ullTotalPhys << ",\n"
           << "  \"compiler\": \""
           << __VERSION__ << "\"\n"
           << "}\n";
    return result.str();
#else
    struct utsname operating_system;
    if (uname(&operating_system) != 0) {
        throw std::runtime_error("Could not read operating-system information.");
    }
    const long logical_processors = sysconf(_SC_NPROCESSORS_ONLN);
    const long physical_pages = sysconf(_SC_PHYS_PAGES);
    const long page_size = sysconf(_SC_PAGESIZE);
    if (logical_processors < 1 || physical_pages < 1 || page_size < 1) {
        throw std::runtime_error("Could not read system capacity information.");
    }
    std::ostringstream result;
    result << "{\n"
           << "  \"os\": \"" << operating_system.sysname << ' '
           << operating_system.release << "\",\n"
           << "  \"architecture\": \"" << operating_system.machine << "\",\n"
           << "  \"logical_processors\": " << logical_processors << ",\n"
           << "  \"physical_memory_bytes\": "
           << static_cast<unsigned long long>(physical_pages) *
                  static_cast<unsigned long long>(page_size) << ",\n"
           << "  \"compiler\": \"" << __VERSION__ << "\"\n"
           << "}\n";
    return result.str();
#endif
}

double percentile(const std::vector<double>& sorted_values, double fraction) {
    if (sorted_values.empty()) {
        return 0.0;
    }
    const std::size_t index = static_cast<std::size_t>(
        std::ceil(fraction * sorted_values.size())) - 1;
    return sorted_values[std::min(index, sorted_values.size() - 1)];
}

Measurement run_once(
    int worker_count,
    const Options& options) {
    const chronos::RetryPolicy retry_policy(
        2, std::chrono::milliseconds(10), std::chrono::milliseconds(10));
    chronos::InMemoryScheduler scheduler(
        static_cast<std::size_t>(worker_count), retry_policy);
    typedef std::chrono::steady_clock Clock;
    const Clock::time_point submitted_at = Clock::now();
    std::vector<double> queue_latencies;
    std::vector<double> task_durations;
    queue_latencies.reserve(options.jobs + 1);
    task_durations.reserve(options.jobs + 1);
    std::mutex sample_mutex;
    std::mutex failure_mutex;
    std::atomic<std::size_t> first_job_attempts(0);
    Clock::time_point first_failure_start;
    Clock::time_point recovery_start;
    Clock::time_point recovery_finish;

    for (std::size_t index = 0; index < options.jobs; ++index) {
        chronos::Job job;
        job.id = "bench-" + std::to_string(worker_count) + "-" +
                 std::to_string(index);
        const bool inject_failure = index == 0;
        const bool accepted = scheduler.submit(job,
            [&, inject_failure]() {
                const Clock::time_point task_start = Clock::now();
                std::size_t job_attempt = 0;
                if (inject_failure) {
                    job_attempt = ++first_job_attempts;
                    std::lock_guard<std::mutex> lock(failure_mutex);
                    if (job_attempt == 1) {
                        first_failure_start = task_start;
                    } else if (job_attempt == 2) {
                        recovery_start = task_start;
                    }
                }

                {
                    std::lock_guard<std::mutex> lock(sample_mutex);
                    queue_latencies.push_back(
                        std::chrono::duration<double, std::milli>(
                            task_start - submitted_at).count());
                }
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(options.duration_ms));
                const Clock::time_point task_finish = Clock::now();
                {
                    std::lock_guard<std::mutex> lock(sample_mutex);
                    task_durations.push_back(
                        std::chrono::duration<double>(
                            task_finish - task_start).count());
                }
                if (inject_failure && job_attempt == 1) {
                    throw std::runtime_error("benchmark injected failure");
                }
                if (inject_failure && job_attempt == 2) {
                    std::lock_guard<std::mutex> lock(failure_mutex);
                    recovery_finish = task_finish;
                }
            });
        if (!accepted) {
            throw std::runtime_error("Benchmark job submission was rejected.");
        }
    }

    for (std::size_t index = 0; index < options.jobs; ++index) {
        const std::string id =
            "bench-" + std::to_string(worker_count) + "-" +
            std::to_string(index);
        const chronos::JobStatus result = scheduler.wait(id);
        if (result.state != chronos::JobState::Succeeded) {
            throw std::runtime_error(
                "Benchmark job did not recover: " + id + " (" + result.error + ")");
        }
    }
    const Clock::time_point finished_at = Clock::now();
    const double elapsed_seconds =
        std::chrono::duration<double>(finished_at - submitted_at).count();
    if (elapsed_seconds <= 0.0) {
        throw std::runtime_error("Benchmark duration was not measurable.");
    }

    std::sort(queue_latencies.begin(), queue_latencies.end());
    double total_task_seconds = 0.0;
    for (std::vector<double>::const_iterator duration = task_durations.begin();
         duration != task_durations.end(); ++duration) {
        total_task_seconds += *duration;
    }

    double recovery_ms = 0.0;
    {
        std::lock_guard<std::mutex> lock(failure_mutex);
        if (first_job_attempts.load() != 2) {
            throw std::runtime_error(
                "Injected failure was not recovered exactly once.");
        }
        recovery_ms = std::chrono::duration<double, std::milli>(
            recovery_finish - first_failure_start).count();
    }

    Measurement measurement;
    measurement.workers = worker_count;
    measurement.jobs = options.jobs;
    measurement.duration_ms = options.duration_ms;
    measurement.throughput = options.jobs / elapsed_seconds;
    measurement.p50 = percentile(queue_latencies, 0.50);
    measurement.p95 = percentile(queue_latencies, 0.95);
    measurement.p99 = percentile(queue_latencies, 0.99);
    measurement.resource_utilization =
        100.0 * total_task_seconds /
        (static_cast<double>(worker_count) * elapsed_seconds);
    measurement.failure_recovery_ms = recovery_ms;
    measurement.elapsed_ms = elapsed_seconds * 1000.0;
    return measurement;
}

}

int main(int argc, char** argv) {
    try {
        const Options options = parse_options(argc, argv);
        const std::string directory = options.output_dir;
#ifdef _WIN32
        const int mkdir_result = _mkdir(directory.c_str());
#else
        const int mkdir_result = mkdir(directory.c_str(), 0755);
#endif
        if (mkdir_result != 0 && errno != EEXIST) {
            throw std::runtime_error(
                "Could not create benchmark output directory: " + directory);
        }

        const std::string specs = machine_specs();
        std::ofstream machine_file(
            (directory + "/machine-specs.json").c_str(),
            std::ios::out | std::ios::trunc);
        if (!machine_file) {
            throw std::runtime_error("Could not open machine-specs.json.");
        }
        machine_file << specs;
        machine_file.close();
        if (!machine_file) {
            throw std::runtime_error("Could not save machine-specs.json.");
        }

        std::ofstream csv(
            (directory + "/chronos_benchmark.csv").c_str(),
            std::ios::out | std::ios::trunc);
        if (!csv) {
            throw std::runtime_error("Could not open chronos_benchmark.csv.");
        }
        csv << "workers,jobs,task_duration_ms,throughput_jobs_per_sec,"
               "scheduling_latency_p50_ms,scheduling_latency_p95_ms,"
               "scheduling_latency_p99_ms,worker_time_utilization_pct,"
               "failure_recovery_ms,elapsed_ms\n";
        csv << std::fixed << std::setprecision(3);

        std::cout << "Machine specifications:\n" << specs;
        for (const int worker_count : {1, 2, 4, 8}) {
            const Measurement measurement = run_once(worker_count, options);
            csv << measurement.workers << ',' << measurement.jobs << ','
                << measurement.duration_ms << ',' << measurement.throughput
                << ',' << measurement.p50 << ',' << measurement.p95 << ','
                << measurement.p99 << ','
                << measurement.resource_utilization << ','
                << measurement.failure_recovery_ms << ','
                << measurement.elapsed_ms << '\n';
            std::cout << worker_count << " workers: "
                      << measurement.throughput << " jobs/s, scheduling p50/"
                      << "p95/p99 " << measurement.p50 << '/'
                      << measurement.p95 << '/' << measurement.p99
                      << " ms, worker-time utilization "
                      << measurement.resource_utilization
                      << "%, failure recovery "
                      << measurement.failure_recovery_ms << " ms\n";
        }
        csv.close();
        if (!csv) {
            throw std::runtime_error("Could not save benchmark CSV results.");
        }
        std::cout << "Saved real benchmark results to "
                  << directory << "/chronos_benchmark.csv\n";
    } catch (const std::exception& error) {
        std::cerr << "Benchmark failed: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
