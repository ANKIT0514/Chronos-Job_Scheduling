#pragma once

#include <librdkafka/rdkafka.h>

#include <memory>
#include <string>
#include <vector>

namespace chronos {

namespace kafka_topics {
extern const char* const job_submission;
extern const char* const job_cancellation;
extern const char* const job_status_updates;
extern const char* const worker_heartbeats;
extern const char* const worker_events;
extern const char* const job_assignments;
}

struct KafkaEvent {
    std::string topic;
    std::string key;
    std::string payload;
};

class KafkaEventProducer {
public:
    explicit KafkaEventProducer(const std::string& brokers);
    ~KafkaEventProducer();

    KafkaEventProducer(const KafkaEventProducer&) = delete;
    KafkaEventProducer& operator=(const KafkaEventProducer&) = delete;

    void publish(
        const std::string& topic,
        const std::string& key,
        const std::string& payload,
        int timeout_ms = 10000);
    void flush(int timeout_ms = 10000);

private:
    rd_kafka_t* producer_;
};

class KafkaEventConsumer {
public:
    KafkaEventConsumer(
        const std::string& brokers,
        const std::string& group_id,
        const std::vector<std::string>& topics);
    ~KafkaEventConsumer();

    KafkaEventConsumer(const KafkaEventConsumer&) = delete;
    KafkaEventConsumer& operator=(const KafkaEventConsumer&) = delete;

    bool poll(KafkaEvent& event, int timeout_ms);
    void acknowledge();

private:
    rd_kafka_t* consumer_;
    std::unique_ptr<rd_kafka_message_t, void (*)(rd_kafka_message_t*)>
        pending_message_;
};

}
