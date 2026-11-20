#include "chronos/scheduler/retry_policy.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <stdexcept>

namespace chronos {
namespace {

TEST(RetryPolicyTest, UsesExponentialBackoffAndCapsTheDelay) {
    const RetryPolicy policy(
        5, std::chrono::milliseconds(10), std::chrono::milliseconds(25));

    EXPECT_EQ(policy.delay_after_attempt(1), std::chrono::milliseconds(10));
    EXPECT_EQ(policy.delay_after_attempt(2), std::chrono::milliseconds(20));
    EXPECT_EQ(policy.delay_after_attempt(3), std::chrono::milliseconds(25));
}

TEST(RetryPolicyTest, AllowsOnlyAttemptsBelowTheConfiguredMaximum) {
    const RetryPolicy policy(
        3, std::chrono::milliseconds(0), std::chrono::milliseconds(0));

    EXPECT_TRUE(policy.should_retry(1));
    EXPECT_TRUE(policy.should_retry(2));
    EXPECT_FALSE(policy.should_retry(3));
    EXPECT_FALSE(policy.should_retry(0));
}

TEST(RetryPolicyTest, RejectsInvalidBoundsAndAttemptNumbers) {
    EXPECT_THROW(RetryPolicy(0), std::invalid_argument);
    EXPECT_THROW(RetryPolicy(
        2, std::chrono::milliseconds(10), std::chrono::milliseconds(5)),
        std::invalid_argument);
    EXPECT_THROW(RetryPolicy(2).delay_after_attempt(0), std::invalid_argument);
}

}
}
