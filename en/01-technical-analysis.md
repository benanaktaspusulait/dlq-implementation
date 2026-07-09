# Kafka Retry and DLQ Technical Analysis: `cmd-adaptor-sns`

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

The two input topics (`landing-1`, `landing-413`) are consumed from the **CDLZ cluster** (`app.cdlz-kafka`, broker `FDP_APP_CDL_KAFKA_BROKER`, group `cdlz-sns`), which is a different cluster from the adaptor's own `app.kafka` cluster (`FDP_KAFKA_BROKER`) that the output topics use. This cluster split matters for the DLQ design: the DLQ topics and DLQ producer must live on the CDLZ cluster, not the adaptor cluster.

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

If the platform requires explicit provisioned names, provide these properties through environment values/secrets instead of relying on runtime string concatenation.

---

## Kafka Retry Design

Retry is a separate decision point from DLQ. The goal is to recover transient failures without creating DLQ records, while sending permanent or malformed payload failures to DLQ without holding the same partition unnecessarily.

| Retry type | When to use | Watch out |
|------------|-------------|-----------|
| Producer retry | Temporary failures while the producer writes to Kafka | Does not replace listener processing retry; the `KafkaTemplate.send()` result still has to be observed |
| Blocking consumer retry | Short transient processing failures | The partition waits on the same record; keep total backoff short |
| Retry-topic pattern | Minute-level delay or downstream outage | Requires extra topics, ACLs, retention, and monitoring |
| DLQ fast path | Schema/serialization/permanent business validation failures | Retrying the same record will not help |

For the first SNS pilot, use blocking consumer retry: for example 3 retries, 1 second initial interval, 2x multiplier, and 30 seconds max interval. If total wait time becomes operationally too long, or downstream outages require minute-level waiting, design a retry-topic pattern in Phase 3.

Exception classification should be explicit:

| Class | Example | Action |
|-------|---------|--------|
| Retryable | `TimeoutException`, temporary broker/network failures, temporary schema registry access failure | Exponential backoff retry |
| Non-retryable | Deserialization, incompatible schema, permanent payload validation failure | DLQ fast path |
| Needs review | EORI multi-send partial failure | Do not use aggressive retry until idempotency/duplicate strategy is clear |

---

## Architecture Decisions by Phase

| Phase | Architecture decision | Not done in this phase |
|-------|-----------------------|------------------------|
| Phase 0: Discovery | Confirm offset commit, CDLZ/adaptor cluster split, `ErrorHandlingDeserializer`, EORI duplicate impact, and exception taxonomy | No runtime behavior changes |
| Phase 1: No Silent Loss | Implement short blocking retry, DLQ, DLQ publish failure alerting, and listener exception propagation | No retry topics, reprocessor, or bulk replay |
| Phase 2: Operational Hardening | Complete dashboards, alerts, runbook, DLQ inspection procedure, and schema mismatch operating decision | No new retry topology |
| Phase 3: Retry Topic Pattern | Design retry topic chain only if lag/downstream outage evidence exists | Do not grow blocking retry without evidence |
| Phase 4: Controlled Reprocessing | Design replay with dry-run, offset range, audit, RBAC, and rate limits | No uncontrolled API or bulk replay |
| Phase 5: Platform Standard | Turn the SNS pilot into shared config/header/metric/runbook standards | Do not copy to other adaptors without validation |

This sequence captures the critical safety gain in Phase 1 while delaying riskier automation until measurement and operational discipline exist.

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
            // partition -1 lets the producer's partitioner pick a valid partition.
            // Pinning record.partition() fails when the DLQ topic has fewer partitions
            // than the source topic. Only pin it when DLQ partitions >= source partitions.
            return new TopicPartition(dlqTopic, -1);
        }
    );

    ExponentialBackOffWithMaxRetries backOff =
        new ExponentialBackOffWithMaxRetries(dlqProperties.retry().maxRetries());
    backOff.setInitialInterval(dlqProperties.retry().intervalMs());
    backOff.setMultiplier(dlqProperties.retry().multiplier());
    backOff.setMaxInterval(dlqProperties.retry().maxIntervalMs());

    DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

    errorHandler.addNotRetryableExceptions(
        org.apache.kafka.common.errors.SerializationException.class,
        org.springframework.kafka.support.serializer.DeserializationException.class,
        IllegalArgumentException.class
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

- Gate this bean on `app.dlq.enabled` (for example with `@ConditionalOnProperty(prefix = "app.dlq", name = "enabled", havingValue = "true", matchIfMissing = true)`) so the `enabled` property and the rollback step actually take effect. Without this, setting `app.dlq.enabled=false` has no runtime effect.
- If Spring Boot auto-configuration does not attach this `DefaultErrorHandler` bean to the listener container factory, explicitly call `setCommonErrorHandler(...)` on the existing factory.
- `dlqKafkaTemplate` must target the **CDLZ** cluster (`app.cdlz-kafka` / `FDP_APP_CDL_KAFKA_BROKER`) with its schema registry, because the listeners consume from that cluster and the recoverer republishes the original consumed record. Using the default adaptor `KafkaTemplate` (which points at `app.kafka` / `FDP_KAFKA_BROKER`) would write the DLQ to the wrong cluster.
- The DLQ producer serializer must support both `GenericRecord` and `CdlzLandingRecord` payloads.
- `addNotRetryableExceptions(DeserializationException.class, ...)` only helps if the consumer factory wraps its Avro deserializers in `ErrorHandlingDeserializer`. Otherwise deserialization/schema failures break the poll loop and never reach this handler or the DLQ. Verify this in `fdp-commons`.
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
| `fdp.kafka.listener.retry.exhausted.total` | `topic`, `exception` | Count records sent to DLQ after retries are exhausted |
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
| Retry policy | Retryable exceptions use retry count/backoff; non-retryable exceptions use DLQ fast path. |
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
