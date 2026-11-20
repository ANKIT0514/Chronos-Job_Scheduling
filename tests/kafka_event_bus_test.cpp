#include "chronos/streaming/kafka_event_bus.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace chronos {
namespace {

TEST(KafkaEventBusTest, PublishesAndConsumesAnEvent) {
    const char* configured_brokers =
        std::getenv("CHRONOS_TEST_KAFKA_BROKERS");
    if (configured_brokers == NULL || *configured_brokers == '\0') {
        GTEST_SKIP() << "Set CHRONOS_TEST_KAFKA_BROKERS for Kafka integration tests.";
    }

    const std::string unique_id = std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
    const std::string topic = "chronos-test-" + unique_id;
    KafkaEventConsumer consumer(
        configured_brokers, "chronos-test-group-" + unique_id,
        std::vector<std::string>{topic});
    KafkaEventProducer producer(configured_brokers);
    producer.publish(topic, "job-1", R"({"state":"PENDING"})");

    const std::chrono::steady_clock::time_point deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(20);
    KafkaEvent event;
    while (std::chrono::steady_clock::now() < deadline) {
        if (consumer.poll(event, 1000)) {
            EXPECT_EQ(event.topic, topic);
            EXPECT_EQ(event.key, "job-1");
            EXPECT_EQ(event.payload, R"({"state":"PENDING"})");
            EXPECT_THROW(consumer.poll(event, 0), std::logic_error);
            consumer.acknowledge();
            EXPECT_THROW(consumer.acknowledge(), std::logic_error);
            return;
        }
    }
    FAIL() << "Timed out waiting for the published Kafka event.";
}

TEST(KafkaEventBusTest, ValidatesEventsAndConsumerConfiguration) {
    EXPECT_THROW(KafkaEventProducer(""), std::invalid_argument);
    EXPECT_THROW(KafkaEventConsumer(
        "localhost:9092", "", std::vector<std::string>{"worker-events"}),
        std::invalid_argument);
    EXPECT_THROW(KafkaEventConsumer(
        "localhost:9092", "group", std::vector<std::string>()),
        std::invalid_argument);
}

}
}
