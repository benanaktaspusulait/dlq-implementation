# Kafka Retry and DLQ Discovery Recommendation

| Field | Value |
|-------|-------|
| Owner | Benan Aktas |
| Status | In Review |
| Created | 2026-07-08 |
| Last updated | 2026-07-09 |
| Scope | `cmd-adaptor-sns` module |
| Labels | kafka, retry, dlq, error-handling, no-silent-loss, data-integrity |

---

## Executive Summary

- **Current risk:** listener-level `try/catch + log.error` in `cmd-adaptor-sns` may allow failed records to be treated as consumed.
- **Current risk:** async `KafkaTemplate.send()` results may not surface producer acknowledgement failures.
- **Current gap:** there is no recoverable failed-record destination and no retry/DLQ telemetry or alert ownership.
- **Recommendation:** complete Phase 0 discovery before implementation; confirm offset commit, cluster, serializer, deserializer, exception taxonomy, and operational ownership decisions first.
- **First delivery objective:** deliver a **No Silent Loss SNS Pilot**, not full platform retry/DLQ standardisation.
- **Scope note:** this is verified for `cmd-adaptor-sns` only; other FDP adaptors require separate validation.

## Key Message

1. Kafka listener failures in `cmd-adaptor-sns` are caught in application code and only logged.
2. That disables the Spring Kafka retry/error-handler path; transient failures are not retried.
3. The immediate architectural concern is **silent loss / silent failure**, not DLQ alone.
4. DLQ is only one part of the wider failure-management design: listener exception propagation, producer send acknowledgement, bounded retry, exception classification, DLQ, metrics, alerting, and later controlled replay.

---

## Evidence Reviewed

| Source | Finding |
|--------|---------|
| `cmd-adaptor-sns/src/main/java/uk/gov/ho/dacc/fdp/listener/KafkaSourceListener.java` | The `@KafkaListener` catches exceptions and closes them with `log.error`. |
| `cmd-adaptor-sns/src/main/java/uk/gov/ho/dacc/fdp/listener/KafkaLookupEoriListener.java` | Same pattern exists in the EORI lookup listener. One landing record can also produce multiple async sends. |
| `cmd-adaptor-sns/src/main/resources/application.yml` | Input topics are `landing-1` and `landing-413`; there is no DLQ topic/property. |
| `cmd-adaptor-sns/src/main/resources/application.yml` | Dynatrace/Prometheus infrastructure exists, but there are no DLQ/retry metrics. |
| `cmd-adaptor-sns-integration-tests/pom.xml` | Integration tests run through docker-compose profiles; no Testcontainers dependency was found. |

> **Note:** Code references in this document point to the `cmd-adaptor-sns` module within the FDP repository. Adjust paths according to your local checkout.

This document is based on findings verified for the SNS adaptor. Other FDP adaptors may use the same pattern, but they should be validated before being included in a wider rollout.

---

## Current State

`cmd-adaptor-sns` consumes two main Kafka inputs: the CDLZ SNS landing stream and the EORI landing stream. Both listeners catch processing failures and log them. Because the exception does not leave the listener method, Spring Kafka retry and `DefaultErrorHandler` behavior cannot take over. In addition, `KafkaTemplate.send()` is asynchronous, so producer acknowledgement failures can be missed unless the send result is awaited or propagated back to the listener thread.

---

## Top Observations

| # | Observation | Impact | Recommendation | Confidence |
|---|-------------|--------|----------------|------------|
| 1 | Listener-level `try/catch + log.error` | Failed records may be committed and lost | Do not swallow errors; let Spring Kafka handle them | High |
| 2 | No Kafka retry policy | Transient failures become data-loss/manual-recovery risks | Define retryable and non-retryable exceptions | High |
| 3 | Producer send result is not awaited | Async send failures may not reach retry/DLQ | Await sends or propagate failures to the listener thread | High |
| 4 | No DLQ topic/property | Records still failing after max retries are not recoverable | Define DLQ topics for consumed input topics | High |
| 5 | No DLQ/retry metric or alert | Operations can only discover failures through logs | Add DLQ and retry metrics | High |
| 6 | No reprocessing workflow | Recovery remains manual and risky | Start with manual procedure; add controlled reprocessor later | Medium |

---

## Why Retry/DLQ Failure Handling?

The purpose of this recommendation is not simply to add a DLQ topic. The purpose is to make failed records visible, recoverable where appropriate, and operationally owned through retry, classification, DLQ preservation, metrics, alerts, and controlled replay governance.

### Data Loss Prevention

Current flow:

```text
Kafka record -> Listener -> Failure -> log.error -> record may complete as consumed
```

Target flow:

```text
Kafka record -> Listener -> exception propagates -> Kafka retry -> still failing -> DLQ -> metric/alert
```

A DLQ helps preserve the failed record with the original payload and failure metadata. It does not by itself solve retry, replay, idempotency, or operational ownership; the failed record becomes potentially replayable only under a controlled, approved procedure.

### Why Kafka Retry Must Be Designed Separately

DLQ is the final stop; transient failures should first be handled by Kafka consumer retry. Short broker, schema registry, network, or downstream availability problems may recover after a few retry attempts. Broken payloads, schema incompatibility, and permanent business validation failures should not be retried indefinitely and should move quickly to DLQ.

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

**Primary recommendation:** after Phase 0 discovery, run a controlled pilot with bounded consumer retry using Spring Kafka `DefaultErrorHandler` + DLQ with `DeadLetterPublishingRecoverer` + enriched metadata.

This uses Spring Kafka listener retry and recovery behavior without building a fully custom error-handling framework. The project-specific critical fix is that the current listener `try/catch` blocks must stop swallowing failures and producer send failures must be surfaced back to the listener thread.

### Retry Policy

| Failure type | Action | Rationale |
|--------------|--------|-----------|
| Temporary Kafka/Schema Registry/network failure | Retry with exponential backoff | Short outages can recover without creating DLQ records |
| Serialization/schema incompatibility | No retry or minimal retry, then DLQ | Retrying the same payload will not fix it |
| Permanent business validation failure | DLQ | Requires data or code correction |
| Long downstream outage | Consider retry-topic pattern | Long blocking retries can hold a partition unnecessarily |

For the first pilot, use short blocking retry. If total backoff would stretch into minutes, design a Spring Kafka retry-topic pattern (`retry-1m`, `retry-5m`, then DLQ) as a later phase.

### DLQ Topic Rule

The DLQ topic should be tied to the topic consumed by the listener, not to the output topic:

| Listener | Consumed topic property | Local default | Recommended DLQ |
|----------|-------------------------|---------------|-----------------|
| `KafkaSourceListener` | `app.topic.cdlz-incoming` | `landing-1` | `${app.topic.cdlz-incoming}-dlq` |
| `KafkaLookupEoriListener` | `app.lookup.eori.cdlz-incoming` | `landing-413` | `${app.lookup.eori.cdlz-incoming}-dlq` |

If the platform does not allow runtime suffix conventions, provide explicit `app.dlq.topics.source` and `app.dlq.topics.eori` properties.

Both consumed topics live on the **CDLZ cluster** (`app.cdlz-kafka`, `FDP_APP_CDL_KAFKA_BROKER`), which is separate from the adaptor's own `app.kafka` cluster (`FDP_KAFKA_BROKER`). The DLQ topics above, and the `KafkaTemplate` used by `DeadLetterPublishingRecoverer`, must be provisioned and configured against the CDLZ cluster, not the adaptor cluster.

---

## Solution Options

| Option | Description | Pros | Cons |
|--------|-------------|------|------|
| A | Blocking retry + `DefaultErrorHandler` + DLQ | Lowest-complexity pilot option | Can hold a partition during longer outages |
| B | Blocking retry + explicit DLQ resolver + metadata + metrics | Recommended first phase; supports audit and alerting | Requires retry classification/testing |
| C | Retry-topic pattern + DLQ | Strong for long backoff/high volume | More topics and operational complexity |

---

## Phased Roadmap

This should not be delivered as one large Kafka platform change. The safer order is to understand current behavior, stop silent loss, then grow operational and replay capability in controlled steps.

### Phase 0: Architecture Discovery

This phase is a hard gate. Do not start Phase 1 implementation until these decisions are documented.

Collect evidence without changing runtime behavior.

- [ ] Confirm offset commit semantics for listener success, listener exception, retry exhausted, DLQ publish success, and DLQ publish failure.
- [ ] Confirm the CDLZ cluster vs adaptor cluster split; decide where DLQ topics and the DLQ `KafkaTemplate` live.
- [ ] Verify whether the CDLZ consumer factory uses `ErrorHandlingDeserializer`.
- [ ] Analyze EORI partial-success and duplicate impact.
- [ ] Write the retryable/non-retryable exception taxonomy.

#### Phase Gate / Decision Checklist

See the full Phase Gate checklist in [Technical Analysis](01-technical-analysis.md#phase-gate--decision-checklist). The gate is a hard stop: Phase 1 must not begin until all items are confirmed.

Exit criteria: error taxonomy, offset commit decision, DLQ cluster decision, and EORI idempotency decision are documented.

### Phase 1: No Silent Loss SNS Pilot

The goal is only to stop silent data loss; retry topics and reprocessor tooling are deliberately out of scope.

- [ ] Remove `try/catch + log.error` or rethrow exceptions.
- [ ] Await producer send futures so async send failures become visible.
- [ ] Add short blocking retry: max retry, exponential backoff, non-retryable fast path.
- [ ] Send the original consumed record to DLQ after retries are exhausted.
- [ ] Add critical metric/alert for DLQ publish failure.
- [ ] Prove with unit and docker-compose integration tests that silent loss is gone.
- [ ] Include EORI in Phase 1 only if its idempotency/duplicate strategy has been approved; otherwise keep it as a documented design item outside the SNS pilot.

Exit criteria: a record is either processed, retried, or preserved in DLQ; no silent `log.error` loss remains.

### Phase 2: Operational Hardening

Make the system operable in production.

- [ ] Add retry count, retry exhausted, DLQ count, and DLQ publish failure metrics to dashboards.
- [ ] Define consumer lag and retry spike alerts.
- [ ] Complete the runbook with real topic, cluster, ACL, and serializer checks.
- [ ] Define DLQ inspection procedure and incident record format.
- [ ] Tie schema evolution and schema mismatch decisions into the runbook.

Exit criteria: the team can detect a retry/DLQ event within 5 minutes and follow the right procedure.

### Phase 3: Retry Topic Pattern

Add this only if there is evidence.

- [ ] Measure whether blocking retry causes partition lag.
- [ ] Confirm whether downstream outages require minute-level waiting.
- [ ] If needed, design `retry-1m`, `retry-5m`, `retry-30m`, final DLQ topic chain.
- [ ] Define ACLs, retention, monitoring, and ownership for every retry topic.

Exit criteria: retry topics are implemented only when blocking retry is proven insufficient.

### Phase 4: Controlled Reprocessing

Replay automation should come late; incorrect replay can create a second incident.

- [ ] Do not build a reprocessor without dry-run, offset range, rate limit, audit log, and authorization.
- [ ] Write replay reason, actor, timestamp, and result to an audit trail.
- [ ] Do not enable bulk replay until EORI duplicate impact is resolved.

Exit criteria: replay can be performed in a controlled, auditable, impact-aware way.

### Phase 5: Platform Standardization

After the SNS pilot succeeds, make the pattern reusable.

- [ ] Design shared retry/DLQ config in `fdp-commons` or a shared template.
- [ ] Standardize metrics, headers, runbook, and dashboard conventions.
- [ ] Onboard other adaptors through the same taxonomy and phase gates.
- [ ] Add schema compatibility and DLQ governance to the platform standard.

Exit criteria: this is no longer an SNS-specific fix but a repeatable FDP adaptor failure-management standard.

---

## Key Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Listener still swallows exceptions | Error handler never runs | Add tests for try/catch removal or rethrow behavior |
| Wrong failure is retried | Poison message can hold the same partition unnecessarily | Add non-retryable exception list and DLQ fast path |
| Retry backoff is too long | Consumer partition is blocked and lag grows | Keep blocking retry short; use retry topics for longer waits |
| Async send failure is not returned to the listener | Producer failure does not reach DLQ | Await the send future or propagate callback failure safely |
| Wrong DLQ serializer | DLQ publish also fails | Add Avro serializer integration test for DLQ |
| EORI partial output | Retry may duplicate lookup records | Define idempotent keys, transactional send, or duplicate handling |
| Wrong DLQ topic naming | Operations cannot find records | Use explicit consumed-topic-to-DLQ mapping |
| DLQ produced to the wrong cluster | Records consumed from the CDLZ cluster cannot be republished/found | Point the DLQ topics and DLQ `KafkaTemplate` at the CDLZ cluster (`FDP_APP_CDL_KAFKA_BROKER`), not the adaptor cluster |
| Deserialization failures bypass the handler | Poison/schema failures stall the poll loop and never reach the DLQ | Wrap consumer Avro deserializers in `ErrorHandlingDeserializer` (in `fdp-commons`) |
| `app.dlq.enabled` flag not wired to a bean | Rollback via the flag has no effect | Gate `DlqConfig` with `@ConditionalOnProperty` on `app.dlq.enabled` |

---

## Architecture Decision Records to Create

| ADR | Decision |
|-----|----------|
| ADR-001 | DLQ topics belong to consumed source topics, not output topics. |
| ADR-002 | DLQ topics and DLQ producer use the CDLZ cluster. |
| ADR-003 | Use short blocking retry for Phase 1 only. |
| ADR-004 | Defer retry topics until lag/downstream outage evidence exists. |
| ADR-005 | Do not automate replay until dry-run, RBAC, audit, rate limiting, and duplicate handling exist. |
| ADR-006 | EORI replay requires an idempotency/duplicate strategy before enablement. |
| ADR-007 | `ErrorHandlingDeserializer` must be confirmed for schema/deserialization DLQ behaviour. |

---

## Review Readiness

This pack is ready for technical review when:

- The evidence has been checked against the latest `cmd-adaptor-sns` code.
- Phase 0 questions are agreed as the first task scope.
- EORI is treated separately unless idempotency and duplicate handling are approved.
- Implementation examples are understood as candidate patterns, not approved code changes.
- Operational ownership for alerting, DLQ triage, and replay approval is identified.

---

## Suggested Task Split

| Task | Purpose |
|------|---------|
| T4.0 Failure Handling Discovery | Confirm current listener/error-handler behaviour, offset commit semantics, cluster split, deserializer behaviour, and exception taxonomy. |
| T4.1 SNS No Silent Loss Pilot | Apply the lowest-risk SNS listener changes only after Phase 0 decisions are complete. |
| T4.2 DLQ Monitoring and Runbook | Add metrics, alerts, dashboards, incident template, and operational ownership. |
| T4.3 EORI Idempotency Assessment | Assess duplicate/partial-success risk before applying retry/DLQ or replay behaviour to EORI. |
| T4.4 Replay Governance | Define dry-run, RBAC, audit, rate limiting, approval, and duplicate handling before any reprocessor is built. |

---

## Related Documents

- [Technical Analysis](01-technical-analysis.md)
- [Implementation Guide](02-implementation-guide.md)
- [Runbook](03-runbook.md)

---

## Glossary

| Term | Definition |
|------|------------|
| DLQ | Dead Letter Queue: a dedicated topic where messages that cannot be processed after retries are stored |
| CDLZ cluster | The Kafka cluster hosting source topics consumed by `cmd-adaptor-sns` (broker: `FDP_APP_CDL_KAFKA_BROKER`) |
| Adaptor cluster | The Kafka cluster used by `cmd-adaptor-sns` for its own output topics (broker: `FDP_KAFKA_BROKER`) |
| Blocking retry | Consumer retry that holds the partition while retrying the same record |
| Non-retryable | An exception that should immediately go to DLQ without further retry attempts |
| ErrorHandlingDeserializer | A Kafka deserializer wrapper that catches deserialization failures and routes them to the error handler |
| DefaultErrorHandler | Spring Kafka's built-in error handler that supports retry and dead-letter publishing |
| DeadLetterPublishingRecoverer | A Spring Kafka recoverer that publishes failed records to a DLQ topic |
