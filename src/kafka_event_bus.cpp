#include "chronos/streaming/kafka_event_bus.hpp"

#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>

namespace chronos {

namespace kafka_topics {
const char* const job_submission = "job-submission";
const char* const job_cancellation = "job-cancellation";
const char* const job_status_updates = "job-status-updates";
const char* const worker_heartbeats = "worker-heartbeats";
const char* const worker_events = "worker-events";
const char* const job_assignments = "job-assignments";
}

namespace {

void set_config(
    rd_kafka_conf_t* config,
    const char* name,
    const std::string& value) {
    char error[512] = {0};
    const rd_kafka_conf_res_t result =
        rd_kafka_conf_set(config, name, value.c_str(), error, sizeof(error));
    if (result != RD_KAFKA_CONF_OK) {
        throw std::invalid_argument(
            std::string("Invalid Kafka configuration: ") + error);
    }
}

std::unique_ptr<rd_kafka_conf_t, void (*)(rd_kafka_conf_t*)> make_config(
    const std::string& brokers,
    const std::string& group_id = std::string()) {
    if (brokers.empty()) {
        throw std::invalid_argument("Kafka broker list must not be empty.");
    }
    rd_kafka_conf_t* raw_config = rd_kafka_conf_new();
    if (raw_config == NULL) {
        throw std::runtime_error("Could not allocate Kafka configuration.");
    }
    std::unique_ptr<rd_kafka_conf_t, void (*)(rd_kafka_conf_t*)> config(
        raw_config, &rd_kafka_conf_destroy);
    set_config(config.get(), "bootstrap.servers", brokers);
    if (!group_id.empty()) {
        set_config(config.get(), "group.id", group_id);
    }
    return config;
}

rd_kafka_t* create_client(
    rd_kafka_type_t type,
    std::unique_ptr<rd_kafka_conf_t, void (*)(rd_kafka_conf_t*)>& config) {
    char error[512] = {0};
    rd_kafka_t* client = rd_kafka_new(
        type, config.get(), error, sizeof(error));
    if (client == NULL) {
        throw std::runtime_error(
            std::string("Could not create Kafka client: ") + error);
    }
    config.release();
    return client;
}

void validate_topic(const std::string& topic) {
    if (topic.empty() || topic.size() > 249) {
        throw std::invalid_argument("Kafka topic name length is invalid.");
    }
    for (std::string::const_iterator it = topic.begin();
         it != topic.end(); ++it) {
        const char character = *it;
        const bool valid =
            (character >= 'A' && character <= 'Z') ||
            (character >= 'a' && character <= 'z') ||
            (character >= '0' && character <= '9') ||
            character == '.' || character == '_' || character == '-';
        if (!valid) {
            throw std::invalid_argument(
                "Kafka topic name contains an invalid character.");
        }
    }
}

}

KafkaEventProducer::KafkaEventProducer(const std::string& brokers)
    : producer_(NULL) {
    std::unique_ptr<rd_kafka_conf_t, void (*)(rd_kafka_conf_t*)> config =
        make_config(brokers);
    set_config(config.get(), "enable.idempotence", "true");
    set_config(config.get(), "acks", "all");
    set_config(config.get(), "message.timeout.ms", "10000");
    producer_ = create_client(RD_KAFKA_PRODUCER, config);
}

KafkaEventProducer::~KafkaEventProducer() {
    if (producer_ != NULL) {
        rd_kafka_flush(producer_, 10000);
        rd_kafka_destroy(producer_);
    }
}

void KafkaEventProducer::publish(
    const std::string& topic,
    const std::string& key,
    const std::string& payload,
    int timeout_ms) {
    validate_topic(topic);
    if (key.empty()) {
        throw std::invalid_argument("Kafka event key must not be empty.");
    }
    if (timeout_ms <= 0) {
        throw std::invalid_argument("Kafka timeout must be positive.");
    }

    const rd_kafka_resp_err_t result = rd_kafka_producev(
        producer_,
        RD_KAFKA_V_TOPIC(topic.c_str()),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_KEY(const_cast<char*>(key.data()), key.size()),
        RD_KAFKA_V_VALUE(const_cast<char*>(payload.data()), payload.size()),
        RD_KAFKA_V_END);
    if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw std::runtime_error(
            std::string("Kafka rejected event: ") + rd_kafka_err2str(result));
    }
    flush(timeout_ms);
}

void KafkaEventProducer::flush(int timeout_ms) {
    if (timeout_ms <= 0) {
        throw std::invalid_argument("Kafka timeout must be positive.");
    }
    const rd_kafka_resp_err_t result =
        rd_kafka_flush(producer_, timeout_ms);
    if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw std::runtime_error(
            std::string("Kafka flush failed: ") + rd_kafka_err2str(result));
    }
}

KafkaEventConsumer::KafkaEventConsumer(
    const std::string& brokers,
    const std::string& group_id,
    const std::vector<std::string>& topics)
    : consumer_(NULL), pending_message_(NULL, &rd_kafka_message_destroy) {
    if (group_id.empty() || topics.empty()) {
        throw std::invalid_argument(
            "Kafka consumer requires a group ID and at least one topic.");
    }
    for (std::vector<std::string>::const_iterator topic = topics.begin();
         topic != topics.end(); ++topic) {
        validate_topic(*topic);
    }
    std::unique_ptr<rd_kafka_conf_t, void (*)(rd_kafka_conf_t*)> config =
        make_config(brokers, group_id);
    set_config(config.get(), "enable.auto.commit", "false");
    set_config(config.get(), "auto.offset.reset", "earliest");
    consumer_ = create_client(RD_KAFKA_CONSUMER, config);
    if (rd_kafka_poll_set_consumer(consumer_) != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_destroy(consumer_);
        consumer_ = NULL;
        throw std::runtime_error("Could not initialize Kafka consumer polling.");
    }

    rd_kafka_topic_partition_list_t* subscription =
        rd_kafka_topic_partition_list_new(
            static_cast<int>(topics.size()));
    if (subscription == NULL) {
        rd_kafka_consumer_close(consumer_);
        rd_kafka_destroy(consumer_);
        consumer_ = NULL;
        throw std::runtime_error("Could not allocate Kafka subscription.");
    }
    for (std::vector<std::string>::const_iterator topic = topics.begin();
         topic != topics.end(); ++topic) {
        rd_kafka_topic_partition_list_add(
            subscription, topic->c_str(), RD_KAFKA_PARTITION_UA);
    }
    const rd_kafka_resp_err_t result =
        rd_kafka_subscribe(consumer_, subscription);
    rd_kafka_topic_partition_list_destroy(subscription);
    if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_consumer_close(consumer_);
        rd_kafka_destroy(consumer_);
        consumer_ = NULL;
        throw std::runtime_error(
            std::string("Could not subscribe to Kafka topics: ") +
            rd_kafka_err2str(result));
    }
}

KafkaEventConsumer::~KafkaEventConsumer() {
    if (consumer_ != NULL) {
        rd_kafka_consumer_close(consumer_);
        rd_kafka_destroy(consumer_);
    }
}

bool KafkaEventConsumer::poll(KafkaEvent& event, int timeout_ms) {
    if (timeout_ms < 0) {
        throw std::invalid_argument("Kafka poll timeout cannot be negative.");
    }
    if (pending_message_) {
        throw std::logic_error(
            "A Kafka message must be acknowledged before polling again.");
    }
    rd_kafka_message_t* message =
        rd_kafka_consumer_poll(consumer_, timeout_ms);
    if (message == NULL) {
        return false;
    }
    if (message->err == RD_KAFKA_RESP_ERR__TIMED_OUT ||
        message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        rd_kafka_message_destroy(message);
        return false;
    }
    if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        const std::string error = rd_kafka_message_errstr(message);
        rd_kafka_message_destroy(message);
        throw std::runtime_error("Kafka consume failed: " + error);
    }

    event.topic = rd_kafka_topic_name(message->rkt);
    if (message->key != NULL) {
        event.key.assign(
            static_cast<const char*>(message->key), message->key_len);
    } else {
        event.key.clear();
    }
    if (message->payload != NULL) {
        event.payload.assign(
            static_cast<const char*>(message->payload), message->len);
    } else {
        event.payload.clear();
    }

    pending_message_.reset(message);
    return true;
}

void KafkaEventConsumer::acknowledge() {
    if (!pending_message_) {
        throw std::logic_error("There is no Kafka message to acknowledge.");
    }
    const rd_kafka_resp_err_t commit_result =
        rd_kafka_commit_message(consumer_, pending_message_.get(), 1);
    if (commit_result != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw std::runtime_error(
            std::string("Kafka offset commit failed: ") +
            rd_kafka_err2str(commit_result));
    }
    pending_message_.reset();
}

}
