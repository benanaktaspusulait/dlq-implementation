# DLQ Technical Analysis: `cmd-adaptor-sns`

---

## Scope

This analysis covers the SNS command adaptor in `/Users/benanaktas/project/home-office/test1`. The main reviewed module is `cmd-adaptor-sns`; the related integration-test module is `cmd-adaptor-sns-integration-tests`.

Other FDP adaptors should only be included after their code patterns have been verified.

---

## Current Flow

```text
CDLZ SNS topic (`landing-1`)
  -> KafkaSourceListener
  -> `fdp-sns-input`
  -> existing mapping/stream pipeline

CDLZ EORI topic (`landing-413`)
  -> KafkaLookupEoriListener
  -> `fdp-sns-lookup-eori`
  -> lookup state-store usage
```

---

## Current Error Handling Code

### `KafkaSourceListener`

```java
@KafkaListener(topics = "${app.topic.cdlz-incoming}")
public void listen(GenericRecord message) {
    try {
        kafkaTemplate.send(kafkaTopics.getAdaptorInputTopic(), message);
    } catch (Exception e) {
        log.error("Error processing message", e);
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
        cdlzLandingRecord.getBody().getEoriData().getEoris().getEori().stream()
            .filter(eoriRecord -> !StringUtils.isEmpty(eoriRecord.getEoriNumber()))
            .forEach(eoriRecord -> kafkaTemplate.send(lookupTopic, ...));
    } catch (Exception e) {
        log.error("Error processing message", e);
    }
}
```

---

## Identified Problems

| # | Problem | File | Impact |
|---|---------|------|--------|
| 1 | Exception does not leave the listener | `KafkaSourceListener`, `KafkaLookupEoriListener` | Spring Kafka retry/DLQ handler does not run |
| 2 | `KafkaTemplate.send()` result is not awaited | Both listeners | Async producer failure can be lost |
| 3 | No DLQ topic/property | `application.yml` | Failed records are not preserved |
| 4 | No error metadata | Listener/config | Weak root cause analysis and audit |
| 5 | No DLQ metric/alert | `application.yml` / monitoring | Operations cannot proactively detect failures |
| 6 | EORI listener emits multiple output records | `KafkaLookupEoriListener` | Retry can create duplicate/partial-success risk |

---

## Current Topic Structure

```yaml
app:
  topic:
    cdlz-incoming: ${FDP_CMD_ADAPTOR_INCOMING_TOPIC:landing-1}
    adaptor-input: fdp-sns-input
  lookup:
    eori:
      cdlz-incoming: ${FDP_CMD_ADAPTOR_INCOMING_EORI_TOPIC:landing-413}
      lookup-topic: fdp-sns-lookup-eori
      cdlz-application-id: fdp-cdlz-sns-lookup-eori-${FDP_APP_KAFKA_TOPIC_SUFFIX:0}
      application-id: fdp-sns-lookup-eori-${FDP_APP_KAFKA_TOPIC_SUFFIX:0}
```

Important correction: the DLQ should be designed around the input topics consumed by the listeners, not the `fdp-sns-input` or `fdp-sns-lookup-eori` output topics. The recoverer writes the original consumed record to the DLQ.

---

## Proposed Configuration

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

If the platform requires explicit provisioned names, provide these properties through environment values/secrets instead of relying on runtime string concatenation.

---

## Listener Changes

For the Spring Kafka error handler to run, the listener must not swallow the failure. Async send failures must also be returned to the listener thread.

### Source Listener

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

### EORI Listener

The EORI listener can produce multiple output records from one landing record, so all send results should be awaited. Partial success and duplicate risks must be assessed separately.

```java
@KafkaListener(
    topics = "${app.lookup.eori.cdlz-incoming}",
    groupId = "${app.lookup.eori.cdlz-application-id}"
)
public void listen(CdlzLandingRecord record) {
    try {
        List<Future<?>> sends = buildLookupRecords(record).stream()
            .map(value -> kafkaTemplate.send(lookupTopic, value.key(), value.payload()))
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

---

## Error Handler Design

Use `DefaultErrorHandler` and an explicit destination resolver. Do not rely on a default recoverer suffix if operations expect specific DLQ topic names.

```java
@Bean
DefaultErrorHandler kafkaErrorHandler(
        KafkaTemplate<Object, Object> dlqKafkaTemplate,
        DlqProperties dlqProperties,
        MeterRegistry meterRegistry) {

    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
        dlqKafkaTemplate,
        (record, exception) -> {
            String dlqTopic = dlqProperties.topicFor(record.topic());
            return new TopicPartition(dlqTopic, record.partition());
        }
    );

    DefaultErrorHandler errorHandler = new DefaultErrorHandler(
        recoverer,
        new FixedBackOff(
            dlqProperties.retry().intervalMs(),
            dlqProperties.retry().maxAttempts()
        )
    );

    errorHandler.setRetryListeners((record, exception, deliveryAttempt) ->
        meterRegistry.counter(
            "fdp.kafka.listener.retry.total",
            "topic", record.topic(),
            "exception", exception.getClass().getSimpleName()
        ).increment()
    );

    return errorHandler;
}
```

Notes:

- If Spring Boot auto-configuration does not attach this `DefaultErrorHandler` bean to the listener container factory, explicitly call `setCommonErrorHandler(...)` on the existing factory.
- The DLQ producer serializer must support both `GenericRecord` and `CdlzLandingRecord` payloads.
- Use Spring Kafka DLT headers where available; add project-specific headers such as `original-topic`, `original-partition`, `original-offset`, `error-class`, `error-message`, and `failed-at` only where needed.

---

## Metadata Header Recommendation

| Header | Description |
|--------|-------------|
| `original-topic` | Kafka topic where the failure occurred |
| `original-partition` | Original partition |
| `original-offset` | Original offset |
| `error-class` | Exception class name |
| `error-message` | Short error message |
| `failed-at` | ISO-8601 timestamp |
| `listener-id` | Listener/container source |
| `delivery-attempt` | Retry attempt count |

Avoid putting full stack traces in headers; they can become too large. Keep stack traces in logs/tracing and put a short error summary in DLQ metadata.

---

## Monitoring

The current `application.yml` already contains Dynatrace and Prometheus endpoint configuration. Add these metrics for the DLQ flow:

| Metric | Dimensions | Purpose |
|--------|------------|---------|
| `fdp.dlq.messages.total` | `topic`, `source_topic`, `exception` | Count records sent to DLQ |
| `fdp.kafka.listener.retry.total` | `topic`, `exception` | Count retry attempts |
| `fdp.dlq.publish.failure.total` | `topic`, `exception` | Detect DLQ publish failures |

Initial alert recommendation:

| Alert | Condition | Severity |
|-------|-----------|----------|
| DLQ Messages Detected | `fdp.dlq.messages.total > 0` in 5 minutes | Warning |
| DLQ Spike | `fdp.dlq.messages.total > 10` in 5 minutes | Critical |
| DLQ Publish Failure | `fdp.dlq.publish.failure.total > 0` | Critical |

---

## Test Strategy

| Level | Test |
|-------|------|
| Unit | Listener does not swallow exceptions; send future failure becomes `IllegalStateException`. |
| Spring context | `DefaultErrorHandler` bean loads and is attached to the listener factory. |
| Serializer | DLQ KafkaTemplate can serialize `GenericRecord` and `CdlzLandingRecord` payloads. |
| Integration | Existing docker-compose profile gets a failing-record scenario; after retry, the record appears in the expected DLQ topic. |
| Regression | Successful records still flow to `fdp-sns-input` and `fdp-sns-lookup-eori`. |

`TopologyTestDriver` is not the right tool for listener-level DLQ behavior. Keep existing stream/topology tests, and add listener/integration coverage for DLQ.

---

## Rollback

1. Disable the new behavior with `app.dlq.enabled=false`.
2. Evaluate the listener exception-propagation change separately in the rollback plan; reverting to the old behavior reintroduces data-loss risk.
3. Do not delete DLQ topics; their records may be needed for incident review and recovery.

---

## Related Documents

- [Why DLQ Should Be Implemented](00-why-dlq-must-be-implemented.md)
- [Implementation Guide](02-implementation-guide.md)
- [Runbook](03-runbook.md)
