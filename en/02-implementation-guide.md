# DLQ Implementation Guide

---

## Preconditions

- Confirm DLQ topic names with the platform team.
- Verify Avro serializer settings for `GenericRecord` and `CdlzLandingRecord` on the DLQ producer.
- Confirm that the listener error handler is not overridden by existing `fdp-commons` Kafka configuration.
- Approve the idempotency strategy for the EORI path, because replaying one failed landing record can duplicate lookup output records.

---

## 1. Add Configuration

`cmd-adaptor-sns/src/main/resources/application.yml`:

```yaml
app:
  dlq:
    enabled: ${FDP_DLQ_ENABLED:true}
    retry:
      interval-ms: ${FDP_DLQ_RETRY_INTERVAL_MS:1000}
      max-attempts: ${FDP_DLQ_RETRY_MAX_ATTEMPTS:3}
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

    public record Retry(long intervalMs, long maxAttempts) {}

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
public class DlqConfig {

    @Bean
    DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<Object, Object> dlqKafkaTemplate,
            DlqProperties properties,
            MeterRegistry meterRegistry) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            dlqKafkaTemplate,
            (record, exception) ->
                new TopicPartition(properties.topicFor(record.topic()), record.partition())
        );

        DefaultErrorHandler handler = new DefaultErrorHandler(
            recoverer,
            new FixedBackOff(
                properties.retry().intervalMs(),
                properties.retry().maxAttempts()
            )
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

---

## 4. Update Listeners

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

## 5. Topic Provisioning

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

## 6. Monitoring

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
| DLQ Spike | 10+ DLQ messages in 5 minutes | Critical |
| DLQ Publish Failure | Any DLQ publish failure | Critical |

---

## 7. Tests

### Unit Tests

- `KafkaSourceListener` throws when producer send fails.
- `KafkaLookupEoriListener` throws when any send future fails.
- Interrupted path resets the thread interrupt flag.
- DLQ topic resolver maps `landing-1 -> landing-1-dlq` and `landing-413 -> landing-413-dlq`.

### Integration Test

Add a failure scenario to the existing docker-compose profile:

1. Send an invalid/unprocessable record to the input topic.
2. Wait for retry attempts to complete.
3. Verify the record appears in the expected DLQ topic.
4. Verify successful records still flow to the normal output topics.

---

## 8. Rollback

```yaml
app:
  dlq:
    enabled: false
```

Do not delete DLQ topics during rollback. Their contents should be preserved for incident analysis and recovery.

---

## Related Documents

- [Why DLQ Should Be Implemented](00-why-dlq-must-be-implemented.md)
- [Technical Analysis](01-technical-analysis.md)
- [Runbook](03-runbook.md)
