# Kafka Retry and DLQ Implementation Guide

---

## Preconditions

- Confirm DLQ topic names with the platform team.
- Verify Avro serializer settings for `GenericRecord` and `CdlzLandingRecord` on the DLQ producer.
- Confirm that the listener error handler is not overridden by existing `fdp-commons` Kafka configuration.
- Agree the retryable and non-retryable exception list with the team.
- Approve the idempotency strategy for the EORI path, because replaying one failed landing record can duplicate lookup output records.
- **Confirm which Kafka cluster the DLQ lives on.** The listeners consume the source topics from the CDLZ cluster (`app.cdlz-kafka`, `FDP_APP_CDL_KAFKA_BROKER`), which is a different cluster from the adaptor's own cluster (`app.kafka`, `FDP_KAFKA_BROKER`). The `DeadLetterPublishingRecoverer` writes the original consumed record, so the DLQ topics and the DLQ `KafkaTemplate` must use the **CDLZ** cluster bootstrap servers and schema registry, not the default adaptor producer.
- **Confirm the consumer uses `ErrorHandlingDeserializer`.** Deserialization and incompatible-schema failures happen during the consumer poll, before the listener runs. Unless the CDLZ consumer factory (owned by `fdp-commons`) wraps the key/value Avro deserializers in `ErrorHandlingDeserializer`, those failures stall the container and never reach `DefaultErrorHandler` or the DLQ, even though they are listed below as the main non-retryable DLQ case.

---

## 1. Add Configuration

`cmd-adaptor-sns/src/main/resources/application.yml`:

```yaml
app:
  dlq:
    enabled: ${FDP_DLQ_ENABLED:true}
    retry:
      mode: ${FDP_DLQ_RETRY_MODE:blocking}
      interval-ms: ${FDP_DLQ_RETRY_INTERVAL_MS:1000}
      max-retries: ${FDP_DLQ_RETRY_MAX_RETRIES:3}
      multiplier: ${FDP_DLQ_RETRY_MULTIPLIER:2.0}
      max-interval-ms: ${FDP_DLQ_RETRY_MAX_INTERVAL_MS:30000}
    topics:
      source-input: ${FDP_CMD_ADAPTOR_INCOMING_TOPIC:landing-1}
      source: ${FDP_CMD_ADAPTOR_SOURCE_DLQ_TOPIC:landing-1-dlq}
      eori-input: ${FDP_CMD_ADAPTOR_INCOMING_EORI_TOPIC:landing-413}
      eori: ${FDP_CMD_ADAPTOR_EORI_DLQ_TOPIC:landing-413-dlq}
```

For production, explicit environment values are safer than `${...}-dlq` concatenation:

```yaml
app:
  dlq:
    topics:
      source-input: ${FDP_CMD_ADAPTOR_INCOMING_TOPIC}
      source: ${FDP_CMD_ADAPTOR_SOURCE_DLQ_TOPIC}
      eori-input: ${FDP_CMD_ADAPTOR_INCOMING_EORI_TOPIC}
      eori: ${FDP_CMD_ADAPTOR_EORI_DLQ_TOPIC}
```

---

## 2. Add DLQ Properties

```java
@ConfigurationProperties(prefix = "app.dlq")
public record DlqProperties(
        boolean enabled,
        Retry retry,
        Topics topics) {

    public String topicFor(String sourceTopic) {
        if (sourceTopic.equals(topics.sourceInput())) {
            return topics.source();
        }
        if (sourceTopic.equals(topics.eoriInput())) {
            return topics.eori();
        }
        return sourceTopic + "-dlq";
    }

    public record Retry(
            String mode,
            long intervalMs,
            long maxRetries,
            double multiplier,
            long maxIntervalMs) {}

    public record Topics(
            String sourceInput,
            String source,
            String eoriInput,
            String eori) {}
}
```

This example shows the mapping logic. In the implementation, `sourceInput` and `eoriInput` can be derived from the existing `app.topic.cdlz-incoming` and `app.lookup.eori.cdlz-incoming` values or supplied explicitly as properties.

---

## 3. Add Error Handler

`cmd-adaptor-sns/src/main/java/uk/gov/ho/dacc/fdp/config/DlqConfig.java`:

```java
@Configuration
@EnableConfigurationProperties(DlqProperties.class)
@ConditionalOnProperty(prefix = "app.dlq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class DlqConfig {

    @Bean
    DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<Object, Object> dlqKafkaTemplate,
            DlqProperties properties,
            MeterRegistry meterRegistry) {

        // Do not pin the DLQ record to the source partition: the DLQ topic may have
        // fewer partitions than the source, and sending to a non-existent partition
        // fails. Return partition -1 so the producer's partitioner chooses. Only pin
        // record.partition() if the DLQ topic is guaranteed to have >= source partitions.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            dlqKafkaTemplate,
            (record, exception) ->
                new TopicPartition(properties.topicFor(record.topic()), -1)
        );

        ExponentialBackOffWithMaxRetries backOff =
            new ExponentialBackOffWithMaxRetries(properties.retry().maxRetries());
        backOff.setInitialInterval(properties.retry().intervalMs());
        backOff.setMultiplier(properties.retry().multiplier());
        backOff.setMaxInterval(properties.retry().maxIntervalMs());

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        handler.addNotRetryableExceptions(
            org.apache.kafka.common.errors.SerializationException.class,
            org.springframework.kafka.support.serializer.DeserializationException.class,
            IllegalArgumentException.class
        );

        handler.setRetryListeners((record, exception, deliveryAttempt) ->
            meterRegistry.counter(
                "fdp.kafka.listener.retry.total",
                "topic", record.topic(),
                "exception", exception.getClass().getSimpleName()
            ).increment()
        );

        return handler;
    }
}
```

If the current Kafka listener container factory does not pick up this bean automatically, add `setCommonErrorHandler(kafkaErrorHandler)` to that factory configuration.

`@ConditionalOnProperty` ties the bean to `app.dlq.enabled`. When it is `false` the bean is not created and the container falls back to Spring's default error handling. `dlqKafkaTemplate` must be a `KafkaTemplate` configured against the **CDLZ** cluster (see Preconditions), because that is where the consumed records and DLQ topics live.

---

## 4. Clarify Retry Policy

Use `mode=blocking` for the first phase. This retries briefly on the same consumer partition and sends the record to DLQ through the recoverer when retries are exhausted.

Rule:

- Retryable: temporary Kafka, network, schema registry access, and timeout failures.
- Non-retryable: deserialization, incompatible schema, permanent payload/business validation failures.
- If longer waiting is needed, do not grow blocking retry; design a Spring Kafka retry-topic pattern with separate topics and ACLs.

Without this distinction, a poison message can hold the same partition repeatedly.

---

## 5. Update Listeners

### `KafkaSourceListener`

```java
@KafkaListener(topics = "${app.topic.cdlz-incoming}")
public void listen(GenericRecord message) {
    try {
        kafkaTemplate.send(kafkaTopics.getAdaptorInputTopic(), message).get();
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while forwarding SNS record", e);
    } catch (ExecutionException e) {
        throw new IllegalStateException("Failed to forward SNS record", e);
    }
}
```

### `KafkaLookupEoriListener`

```java
@KafkaListener(
    topics = "${app.lookup.eori.cdlz-incoming}",
    groupId = "${app.lookup.eori.cdlz-application-id}"
)
public void listen(CdlzLandingRecord cdlzLandingRecord) {
    try {
        List<Future<?>> sends = cdlzLandingRecord.getBody().getEoriData().getEoris().getEori()
            .stream()
            .filter(eoriRecord -> !StringUtils.isEmpty(eoriRecord.getEoriNumber()))
            .map(eoriRecord -> kafkaTemplate.send(
                lookupTopic,
                String.valueOf(eoriRecord.getEoriNumber()),
                toLookupEoriValue(cdlzLandingRecord, eoriRecord)
            ))
            .toList();

        for (Future<?> send : sends) {
            send.get();
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while publishing EORI lookup records", e);
    } catch (ExecutionException e) {
        throw new IllegalStateException("Failed to publish EORI lookup records", e);
    }
}
```

`toLookupEoriValue(...)` is the existing builder logic extracted into a small private method for readability, not a behavioral change.

---

## 6. Topic Provisioning

Local/default topic names:

```bash
kafka-topics.sh --create \
  --topic landing-1-dlq \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server kafka:29092

kafka-topics.sh --create \
  --topic landing-413-dlq \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server kafka:29092
```

For SIT/UAT/PROD:

- Partition count should match the source topic where possible.
- Retention should cover the incident investigation window.
- ACLs must allow the command adaptor to produce to the DLQ topic and operations/reprocessor tooling to consume from it.

---

## 7. Monitoring

The existing Dynatrace config lives under `management.dynatrace.metrics.export`. Publish new metrics via `MeterRegistry`:

```java
meterRegistry.counter(
    "fdp.dlq.messages.total",
    "topic", dlqTopic,
    "source_topic", record.topic(),
    "exception", exception.getClass().getSimpleName()
).increment();
```

Suggested alerts:

| Alert | Condition | Severity |
|-------|-----------|----------|
| DLQ Messages Detected | At least 1 DLQ message in 5 minutes | Warning |
| Retry Spike | Sudden increase in retry attempts | Warning |
| DLQ Spike | 10+ DLQ messages in 5 minutes | Critical |
| DLQ Publish Failure | Any DLQ publish failure | Critical |

---

## 8. Tests

### Unit Tests

- `KafkaSourceListener` throws when producer send fails.
- `KafkaLookupEoriListener` throws when any send future fails.
- Interrupted path resets the thread interrupt flag.
- Retryable exceptions use the expected retry count.
- Non-retryable exceptions use DLQ fast path without waiting for retries.
- DLQ topic resolver maps `landing-1 -> landing-1-dlq` and `landing-413 -> landing-413-dlq`.

### Integration Test

Add a failure scenario to the existing docker-compose profile:

1. Send an invalid/unprocessable record to the input topic.
2. Wait for retry attempts to complete.
3. Verify the record appears in the expected DLQ topic.
4. Verify successful records still flow to the normal output topics.

---

## 9. Rollback

```yaml
app:
  dlq:
    enabled: false
```

Setting `enabled: false` removes the `DlqConfig` bean (via `@ConditionalOnProperty`) so the custom error handler and DLQ recoverer no longer run. This does not revert the listener exception-propagation change; with the error handler gone, the container's default handling applies and reverting the listeners to swallow-and-log reintroduces the original data-loss risk. Decide both parts explicitly in the rollback plan.

Do not delete DLQ topics during rollback. Their contents should be preserved for incident analysis and recovery.

---

## Related Documents

- [Why DLQ Should Be Implemented](00-why-dlq-must-be-implemented.md)
- [Technical Analysis](01-technical-analysis.md)
- [Runbook](03-runbook.md)
