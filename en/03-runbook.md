# Kafka Retry and DLQ Operational Runbook

---

## Status

This runbook describes the operational procedure after Kafka retry and DLQ implementation is enabled. If a reprocessing API has not been built yet, do not assume API endpoints exist; manual replay must use an approved operational procedure.

---

## Quick Reference

| Situation | First Action | Priority |
|-----------|--------------|----------|
| Messages in DLQ | Check topic, exception, and deployment timing | High |
| Retry spike | Check exception type and source topic lag | High |
| Retry exhausted | Inspect records sent to DLQ after max retries | High |
| DLQ publish failure | Check Kafka ACLs, serializer, and topic existence | Critical |
| Retry count rising but no DLQ | Check listener error handler and retry config | High |
| Reprocessed record returns to DLQ | Stop replay until root cause is fixed | High |

---

## DLQ Topics

| Flow | Source topic | DLQ topic |
|------|--------------|-----------|
| SNS landing | `${app.topic.cdlz-incoming}` / local `landing-1` | `landing-1-dlq` or explicit `FDP_CMD_ADAPTOR_SOURCE_DLQ_TOPIC` |
| EORI landing | `${app.lookup.eori.cdlz-incoming}` / local `landing-413` | `landing-413-dlq` or explicit `FDP_CMD_ADAPTOR_EORI_DLQ_TOPIC` |

---

## Monitoring

### Metrics

| Metric | Normal | Warning | Critical |
|--------|--------|---------|----------|
| `fdp.dlq.messages.total` | 0 | > 0 in 5 minutes | > 10 in 5 minutes |
| `fdp.kafka.listener.retry.total` | Low/variable | Sudden increase | Sustained increase |
| `fdp.kafka.listener.retry.exhausted.total` | 0 | > 0 in 5 minutes | > 10 in 5 minutes |
| `fdp.dlq.publish.failure.total` | 0 | > 0 | > 0 |

### First Checks

1. Which source topic is causing the DLQ increase?
2. What is the exception type?
3. Was there a recent deployment or configuration change?
4. Are Schema Registry, Kafka ACLs, and downstream services healthy?
5. Is there duplicate/partial-output risk in the EORI flow?

---

## Alert Response

### Alert: Retry Spike

A retry spike is not always an incident; a transient dependency may be recovering. Still check:

1. Which source topic and exception type is driving retries?
2. Are records succeeding after retry, or is `retry.exhausted` increasing?
3. Is consumer lag increasing?
4. Is a non-retryable failure being retried by mistake?
5. If total backoff is holding partitions unnecessarily, raise the need for a retry-topic pattern.

### Alert: Retry Exhausted

This alert means records are moving to DLQ after max retries.

1. Group DLQ messages by exception type.
2. Check for recent deploy/config/schema changes for the same exception.
3. If the failure is transient, evaluate whether retry count/backoff is sufficient.
4. If the failure is permanent, add a non-retryable fast path to the retry policy.

### Alert: DLQ Messages Detected

1. Check metric dimensions: `topic`, `source_topic`, `exception`.
2. Review application logs and traces for the same time window.
3. Classify the failure as transient or permanent.
4. Do not start replay until the root cause is fixed.
5. Add DLQ topic, partition, offset, and exception details to the incident record.

### Alert: DLQ Publish Failure

Treat this alert as critical because failed records may not be reaching the DLQ.

Checks:

- Does the DLQ topic exist **on the CDLZ cluster** (`FDP_APP_CDL_KAFKA_BROKER`)?
- Is the DLQ KafkaTemplate pointed at the CDLZ cluster and its schema registry?
- Does the command adaptor have produce ACLs for the DLQ topic?
- Does the DLQ KafkaTemplate serializer support the payload type?
- Is the DLQ partition count compatible with the source topic (or is the recoverer using partition `-1`)?

---

## DLQ Inspection Commands

Check topic list:

```bash
kafka-topics.sh --list \
  --bootstrap-server kafka:29092
```

Check DLQ topic details:

```bash
kafka-topics.sh --describe \
  --topic landing-1-dlq \
  --bootstrap-server kafka:29092
```

Inspect DLQ messages:

```bash
kafka-console-consumer.sh \
  --topic landing-1-dlq \
  --from-beginning \
  --max-messages 10 \
  --bootstrap-server kafka:29092
```

In production, check access, data sensitivity, and audit requirements before running these commands.

---

## Reprocessing Procedure

Only reprocess after the root cause has been fixed.

### If No Reprocessor Exists

1. Do not delete DLQ records.
2. Add the relevant partition/offset list to the incident record.
3. Use an approved replay script or platform procedure.
4. Run a dry-run and record-count check before replay.
5. After replay, verify DLQ count and normal output topics.

### If a Reprocessor Has Been Built

Do not use the reprocessor unless it supports:

- Dry-run mode
- Single-record or offset-range selection
- Rate limiting
- Audit logging
- Authorization
- Duplicate handling strategy

---

## Troubleshooting

### Messages Are Not Reaching DLQ

Checklist:

- [ ] Is the listener still swallowing exceptions?
- [ ] Is the `KafkaTemplate.send()` future awaited?
- [ ] Is `DefaultErrorHandler` attached to the listener container factory?
- [ ] Is `app.dlq.enabled=true`? (When `false`, the `DlqConfig` bean is not created.)
- [ ] For deserialization/schema failures: is the consumer using `ErrorHandlingDeserializer`? Without it, these failures break the poll loop and never reach the error handler or DLQ.
- [ ] Does the DLQ topic exist **on the CDLZ cluster** (where the source topics are consumed), not the adaptor cluster?
- [ ] Is the DLQ `KafkaTemplate` pointed at the CDLZ cluster and its schema registry?
- [ ] Does the DLQ topic have at least as many partitions as the source (or is the recoverer using partition `-1`)?
- [ ] Is the DLQ producer serializer correct?
- [ ] Is produce ACL granted?

### Retry Runs Too Often

Checklist:

- [ ] Is the exception type really retryable?
- [ ] Is the same payload repeatedly producing the same exception?
- [ ] Are `max-retries`, `interval-ms`, `multiplier`, and `max-interval-ms` set as expected?
- [ ] Is consumer lag increasing?
- [ ] If longer waiting is needed, would a retry-topic pattern be more appropriate?

### Too Many DLQ Messages

Checklist:

- [ ] Is one exception type dominant?
- [ ] Did it start after the latest deployment?
- [ ] Was there a schema change?
- [ ] Did the source topic receive unexpected payloads?
- [ ] Are downstream Kafka/Schema Registry services reachable?

### Reprocessing Fails

1. Stop replay.
2. Compare the exception type on the new DLQ records.
3. If it is a permanent data/schema issue, do not retry until migration or a code fix is ready.
4. If duplicate output is possible, assess consumer/idempotency impact.

---

## Rollback

1. Disable DLQ behavior with `app.dlq.enabled=false`.
2. Retry config should also be reverted or disabled with `max-retries=0`.
3. Evaluate the new listener exception-propagation behavior explicitly; reverting to the old pattern reintroduces data-loss risk.
4. Do not delete DLQ topics or messages.

---

## Related Documents

- [Why DLQ Should Be Implemented](00-why-dlq-must-be-implemented.md)
- [Technical Analysis](01-technical-analysis.md)
- [Implementation Guide](02-implementation-guide.md)
