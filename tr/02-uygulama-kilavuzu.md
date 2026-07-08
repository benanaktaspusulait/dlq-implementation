# DLQ Uygulama Kılavuzu

---

## Ön Koşullar

- DLQ topic isimleri platform ekibiyle netleştirilmeli.
- DLQ producer'ın Avro serializer ayarları `GenericRecord` ve `CdlzLandingRecord` için doğrulanmalı.
- Listener error handler'ın mevcut `fdp-commons` Kafka configuration tarafından ezilmediği kontrol edilmeli.
- Başarısız EORI landing kaydının tekrar işlenmesi duplicate lookup kayıtları üretebilir; key/idempotency yaklaşımı onaylanmalı.

---

## 1. Konfigürasyon Ekle

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

Production ortamında `${...}-dlq` concatenation yerine explicit env value kullanılması daha güvenlidir:

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

## 2. DLQ Properties Sınıfı Ekle

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

Bu örnek mapping mantığını gösterir. Uygulamada `sourceInput` ve `eoriInput` mevcut `app.topic.cdlz-incoming` ve `app.lookup.eori.cdlz-incoming` değerlerinden türetilebilir ya da property olarak açıkça verilebilir.

---

## 3. Error Handler Ekle

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

Eğer mevcut Kafka listener container factory bu bean'i otomatik kullanmıyorsa ilgili factory config içinde `setCommonErrorHandler(kafkaErrorHandler)` çağrısı eklenmelidir.

---

## 4. Listener'ları Güncelle

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

`toLookupEoriValue(...)` mevcut builder kodunun küçük bir private metoda taşınmış halidir; davranış değişikliği değil, okunabilirlik için önerilir.

---

## 5. Topic Provisioning

Local/default topic isimleri:

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

SIT/UAT/PROD için:

- Partition sayısı source topic ile uyumlu olmalı.
- Retention en az incident inceleme süresini karşılamalı.
- ACL'ler command adaptor'ın DLQ topic'e produce etmesine ve operasyon/reprocessor'ın consume etmesine izin vermeli.

---

## 6. Monitoring

Mevcut Dynatrace config `management.dynatrace.metrics.export` altında bulunuyor. Yeni metric'ler `MeterRegistry` üzerinden yayınlanmalıdır:

```java
meterRegistry.counter(
    "fdp.dlq.messages.total",
    "topic", dlqTopic,
    "source_topic", record.topic(),
    "exception", exception.getClass().getSimpleName()
).increment();
```

Önerilen alertler:

| Alert | Koşul | Severity |
|-------|-------|----------|
| DLQ Messages Detected | 5 dakikada en az 1 DLQ mesajı | Warning |
| DLQ Spike | 5 dakikada 10+ DLQ mesajı | Critical |
| DLQ Publish Failure | DLQ publish başarısızlığı | Critical |

---

## 7. Testler

### Unit Testler

- `KafkaSourceListener` producer failure aldığında exception fırlatmalı.
- `KafkaLookupEoriListener` herhangi bir send future failure aldığında exception fırlatmalı.
- Interrupted durumda thread interrupt flag geri set edilmeli.
- DLQ topic resolver `landing-1 -> landing-1-dlq` ve `landing-413 -> landing-413-dlq` mapping'ini doğru yapmalı.

### Entegrasyon Testi

Mevcut docker-compose profiline bir failure senaryosu eklenmelidir:

1. Test input topic'e hatalı/işlenemeyen kayıt gönder.
2. Retry denemelerinin tamamlanmasını bekle.
3. Kaydın ilgili DLQ topic'e yazıldığını doğrula.
4. Başarılı kayıtların normal output topic'lere gitmeye devam ettiğini doğrula.

---

## 8. Rollback

```yaml
app:
  dlq:
    enabled: false
```

Rollback sırasında DLQ topic'leri silinmemelidir. İçerik, incident analizi ve veri kurtarma için korunmalıdır.

---

## İlgili Belgeler

- [DLQ Neden Yapılmalı](00-dlq-neden-yapilmali.md)
- [Teknik Analiz](01-teknik-analiz.md)
- [Runbook](03-runbook.md)
