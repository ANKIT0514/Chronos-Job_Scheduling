#include "chronos/api/api_gateway.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>

namespace chronos {
namespace {

class ApiGatewayTest : public ::testing::Test {
protected:
    ApiGatewayTest()
        : scheduler_(1, RetryPolicy(
              1, std::chrono::milliseconds(0), std::chrono::milliseconds(0))),
          port_(0) {
        config_.api_key = "test-key";
        config_.requests_per_minute = 100;
        gateway_.reset(new ApiGateway(scheduler_, config_));
        port_ = gateway_->start("127.0.0.1");
    }

    ~ApiGatewayTest() override {
        gateway_->stop();
    }

    httplib::Client client() const {
        return httplib::Client("127.0.0.1", port_);
    }

    httplib::Headers headers() const {
        return httplib::Headers{{"X-API-Key", "test-key"}};
    }

    InMemoryScheduler scheduler_;
    ApiConfig config_;
    int port_;
    std::unique_ptr<ApiGateway> gateway_;
};

TEST_F(ApiGatewayTest, RequiresApiKeyAndValidatesSubmission) {
    httplib::Client http = client();
    const std::string body =
        R"({"id":"job-1","priority":5,"resources":{"cpu_millis":100,"memory_bytes":1024,"gpu_count":0}})";

    const httplib::Result unauthorized = http.Post("/jobs", body, "application/json");
    ASSERT_TRUE(unauthorized);
    EXPECT_EQ(unauthorized->status, 401);

    httplib::Headers auth = headers();
    const httplib::Result invalid =
        http.Post("/jobs", auth, R"({"id":"job-1","priority":-1})",
                  "application/json");
    ASSERT_TRUE(invalid);
    EXPECT_EQ(invalid->status, 400);

    const httplib::Result submitted =
        http.Post("/jobs", auth, body, "application/json");
    ASSERT_TRUE(submitted);
    EXPECT_EQ(submitted->status, 202);
    EXPECT_EQ(scheduler_.wait("job-1").state, JobState::Succeeded);

    const httplib::Result queried = http.Get("/jobs/job-1", auth);
    ASSERT_TRUE(queried);
    EXPECT_EQ(queried->status, 200);
    EXPECT_NE(queried->body.find("SUCCEEDED"), std::string::npos);
}

TEST_F(ApiGatewayTest, RejectsInvalidIdsRangesAndDuplicateSubmissions) {
    httplib::Client http = client();
    const httplib::Headers auth = headers();
    const httplib::Result invalid_id = http.Post(
        "/jobs", auth,
        R"({"id":"bad id","priority":1,"resources":{"cpu_millis":0,"memory_bytes":0,"gpu_count":0}})",
        "application/json");
    ASSERT_TRUE(invalid_id);
    EXPECT_EQ(invalid_id->status, 400);

    const std::string body =
        R"({"id":"same","priority":1,"resources":{"cpu_millis":0,"memory_bytes":0,"gpu_count":0}})";
    ASSERT_EQ(http.Post("/jobs", auth, body, "application/json")->status, 202);
    EXPECT_EQ(http.Post("/jobs", auth, body, "application/json")->status, 409);
}

TEST_F(ApiGatewayTest, CancelsPendingJobsAndReportsUnknownJobs) {
    httplib::Client http = client();
    const httplib::Headers auth = headers();
    const std::string busy =
        R"({"id":"busy","priority":1,"duration_ms":300,"resources":{"cpu_millis":0,"memory_bytes":0,"gpu_count":0}})";
    const std::string pending =
        R"({"id":"pending","priority":1,"resources":{"cpu_millis":0,"memory_bytes":0,"gpu_count":0}})";
    ASSERT_EQ(http.Post("/jobs", auth, busy, "application/json")->status, 202);
    ASSERT_EQ(http.Post("/jobs", auth, pending, "application/json")->status, 202);

    const httplib::Result cancelled = http.Delete("/jobs/pending", auth);
    ASSERT_TRUE(cancelled);
    EXPECT_EQ(cancelled->status, 202);
    EXPECT_EQ(scheduler_.wait("pending").state, JobState::Cancelled);
    EXPECT_EQ(scheduler_.wait("busy").state, JobState::Succeeded);

    const httplib::Result unknown = http.Get("/jobs/missing", auth);
    ASSERT_TRUE(unknown);
    EXPECT_EQ(unknown->status, 404);
}

TEST_F(ApiGatewayTest, ExposesPrometheusMetricsAndStructuredJobLogs) {
    httplib::Client http = client();
    const httplib::Headers auth = headers();
    const std::string body =
        R"({"id":"metrics-job","priority":1,"resources":{"cpu_millis":0,"memory_bytes":0,"gpu_count":0}})";
    ASSERT_EQ(http.Post("/jobs", auth, body, "application/json")->status, 202);
    ASSERT_EQ(scheduler_.wait("metrics-job").state, JobState::Succeeded);
    ASSERT_EQ(http.Get("/jobs/missing", auth)->status, 404);
    ASSERT_EQ(http.Get("/jobs/metrics-job")->status, 401);

    const httplib::Result metrics = http.Get("/metrics");
    ASSERT_TRUE(metrics);
    EXPECT_EQ(metrics->status, 200);
    EXPECT_NE(metrics->body.find("chronos_http_requests_total 3"),
              std::string::npos);
    EXPECT_NE(metrics->body.find("chronos_http_rejected_total 1"),
              std::string::npos);
    EXPECT_NE(metrics->body.find("chronos_jobs_submitted_total 1"),
              std::string::npos);
    EXPECT_NE(metrics->get_header_value("Content-Type").find("text/plain"),
              std::string::npos);
}

TEST(ApiGatewayStandaloneTest, EnforcesRateLimitsAndRejectsBadConfiguration) {
    InMemoryScheduler scheduler(1);
    ApiConfig config;
    config.api_key = "key";
    config.requests_per_minute = 1;
    ApiGateway gateway(scheduler, config);
    const int port = gateway.start("127.0.0.1");
    httplib::Client http("127.0.0.1", port);
    const httplib::Headers auth{{"X-API-Key", "key"}};

    const httplib::Result first = http.Get("/jobs/unknown", auth);
    const httplib::Result limited = http.Get("/jobs/unknown", auth);
    ASSERT_TRUE(first);
    ASSERT_TRUE(limited);
    EXPECT_EQ(first->status, 404);
    EXPECT_EQ(limited->status, 429);

    gateway.stop();
    EXPECT_THROW(ApiGateway(scheduler, ApiConfig()), std::invalid_argument);
}

}
}
