# Dead Letter Queue (DLQ) Recommendation: Why It Should Be Implemented

| Field | Value |
|-------|-------|
| Owner | Benan Aktas |
| Status | In Review |
| Created | 2026-07-08 |
| Last updated | 2026-07-09 |
| Scope | `cmd-adaptor-sns` in `/Users/benanaktas/project/home-office/test1` |
| Labels | kafka, dlq, error-handling, data-integrity |

---

## Key Message

1. Kafka listener failures in `cmd-adaptor-sns` are caught in application code and only logged.
2. That disables the Spring Kafka retry/error-handler path; failed records are not written to a recoverable location.
3. DLQ, retry, metrics, and a runbook should be implemented together to reduce data-loss risk and improve operational visibility.

---

## Evidence Reviewed

| Source | Finding |
|--------|---------|
| `cmd-adaptor-sns/src/main/java/uk/gov/ho/dacc/fdp/listener/KafkaSourceListener.java` | The `@KafkaListener` catches exceptions and closes them with `log.error`. |
| `cmd-adaptor-sns/src/main/java/uk/gov/ho/dacc/fdp/listener/KafkaLookupEoriListener.java` | Same pattern exists in the EORI lookup listener. One landing record can also produce multiple async sends. |
| `cmd-adaptor-sns/src/main/resources/application.yml` | Input topics are `landing-1` and `landing-413`; there is no DLQ topic/property. |
| `cmd-adaptor-sns/src/main/resources/application.yml` | Dynatrace/Prometheus infrastructure exists, but there are no DLQ/retry metrics. |
| `cmd-adaptor-sns-integration-tests/pom.xml` | Integration tests run through docker-compose profiles; no Testcontainers dependency was found. |

This document is based on findings verified for the SNS adaptor. Other FDP adaptors may use the same pattern, but they should be validated before being included in a wider rollout.

---

## Current State

`cmd-adaptor-sns` consumes two main Kafka inputs: the CDLZ SNS landing stream and the EORI landing stream. Both listeners catch processing failures and log them. Because the exception does not leave the listener method, Spring Kafka retry and `DefaultErrorHandler` behavior cannot take over. In addition, `KafkaTemplate.send()` is asynchronous, so producer acknowledgement failures can be missed unless the send result is awaited or propagated back to the listener thread.

---

## Top Observations

| # | Observation | Impact | Recommendation | Confidence |
|---|-------------|--------|----------------|------------|
| 1 | Listener-level `try/catch + log.error` | Failed records may be committed and lost | Do not swallow errors; let Spring Kafka handle them | High |
| 2 | No DLQ topic/property | Failed records are not recoverable | Define DLQ topics for consumed input topics | High |
| 3 | Producer send result is not awaited | Async send failures may not reach retry/DLQ | Await sends or propagate failures to the listener thread | High |
| 4 | No DLQ metric/alert | Operations can only discover failures through logs | Add DLQ and retry metrics | High |
| 5 | No reprocessing workflow | Recovery remains manual and risky | Start with manual procedure; add controlled reprocessor later | Medium |

---

## Why DLQ?

### Data Loss Prevention

Current flow:

```text
Kafka record -> Listener -> Failure -> log.error -> record may complete as consumed
```

Target flow:

```text
Kafka record -> Listener -> exception propagates -> retry -> still failing -> DLQ -> metric/alert
```

A DLQ preserves the failed record with the original payload and failure metadata. The failed record becomes inspectable, recoverable, and replayable under controlled conditions.

### Operational Visibility

| Area | Current | With DLQ |
|------|---------|----------|
| Failure discovery | Manual log review | Metric and alert |
| Lost record count | Unknown | Measurable through DLQ count |
| Root cause analysis | Log/stack-trace dependent | Header metadata + payload |
| Recovery | Source-system resend | DLQ replay/reprocess |

### Audit and Investigation

If DLQ records include original topic, partition, offset, timestamp, exception type, and error message, the team can answer "which record failed and why?" without relying only on application log retention.

---

## Recommended Solution

**Primary recommendation:** Spring Kafka `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` + enriched DLQ metadata.

This uses Spring Kafka listener retry and recovery behavior without building a fully custom error-handling framework. The project-specific critical fix is that the current listener `try/catch` blocks must stop swallowing failures and producer send failures must be surfaced back to the listener thread.

### DLQ Topic Rule

The DLQ topic should be tied to the topic consumed by the listener, not to the output topic:

| Listener | Consumed topic property | Local default | Recommended DLQ |
|----------|-------------------------|---------------|-----------------|
| `KafkaSourceListener` | `app.topic.cdlz-incoming` | `landing-1` | `${app.topic.cdlz-incoming}-dlq` |
| `KafkaLookupEoriListener` | `app.lookup.eori.cdlz-incoming` | `landing-413` | `${app.lookup.eori.cdlz-incoming}-dlq` |

If the platform does not allow runtime suffix conventions, provide explicit `app.dlq.topics.source` and `app.dlq.topics.eori` properties.

---

## Solution Options

| Option | Description | Pros | Cons |
|--------|-------------|------|------|
| A | `DefaultErrorHandler` + DLQ only | Fastest pilot, minimal code | Metadata/metrics may be limited |
| B | `DefaultErrorHandler` + explicit DLQ resolver + metadata + metrics | Recommended balance; supports audit and alerting | Slightly more configuration/testing |
| C | Retry-topic pattern + DLQ | Strong for long backoff/high volume | More topics and operational complexity |

---

## Phased Plan

### Phase 1: SNS Pilot

- [ ] Add DLQ configuration properties in `cmd-adaptor-sns`.
- [ ] Remove or rethrow the failure-swallowing `try/catch` pattern in `KafkaSourceListener` and `KafkaLookupEoriListener`.
- [ ] Await producer send futures so async send failures are visible.
- [ ] Configure retry + DLQ using `DefaultErrorHandler` and `DeadLetterPublishingRecoverer`.
- [ ] Verify the DLQ payload serializer works for `GenericRecord` and `CdlzLandingRecord`.
- [ ] Unit test that listeners do not swallow exceptions and the error-handler bean is loaded.
- [ ] Add a DLQ scenario to the existing docker-compose integration profile.

### Phase 2: Monitoring and Runbook

- [ ] Add metrics such as `fdp.dlq.messages.total` and `fdp.kafka.listener.retry.total`.
- [ ] Define Dynatrace/Prometheus dashboards and alert thresholds.
- [ ] Update the runbook with real topic names, alert names, and the manual replay process.

### Phase 3: Controlled Reprocessing

- [ ] Design a dedicated DLQ reprocessor if manual replay is not enough.
- [ ] Add audit logging, authorization, dry-run, and rate limiting.
- [ ] Address the partial-success/duplicate risk in the EORI listener's multi-send path.

---

## Key Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Listener still swallows exceptions | Error handler never runs | Add tests for try/catch removal or rethrow behavior |
| Async send failure is not returned to the listener | Producer failure does not reach DLQ | Await the send future or propagate callback failure safely |
| Wrong DLQ serializer | DLQ publish also fails | Add Avro serializer integration test for DLQ |
| EORI partial output | Retry may duplicate lookup records | Define idempotent keys, transactional send, or duplicate handling |
| Wrong DLQ topic naming | Operations cannot find records | Use explicit consumed-topic-to-DLQ mapping |

---

## Related Documents

- [Technical Analysis](01-technical-analysis.md)
- [Implementation Guide](02-implementation-guide.md)
- [Runbook](03-runbook.md)
