# DLQ Teknik Analiz: `cmd-adaptor-sns`

---

## Kapsam

Bu analiz `/Users/benanaktas/project/home-office/test1` projesindeki SNS command adaptor için hazırlanmıştır. İncelenen ana modül `cmd-adaptor-sns`, ilgili entegrasyon test modülü ise `cmd-adaptor-sns-integration-tests`tir.

Diğer FDP adaptörleri için aynı öneri ancak kod pattern'i doğrulandıktan sonra genelleştirilmelidir.

---

## Mevcut Akış

```text
CDLZ SNS topic (`landing-1`)
  -> KafkaSourceListener
  -> `fdp-sns-input`
  -> mevcut mapping/stream pipeline

CDLZ EORI topic (`landing-413`)
  -> KafkaLookupEoriListener
  -> `fdp-sns-lookup-eori`
  -> lookup state store kullanımı
```

---

## Mevcut Error Handling Kodu

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

## Tespit Edilen Problemler

| # | Problem | Dosya | Etki |
|---|---------|-------|------|
| 1 | Exception listener dışına çıkmıyor | `KafkaSourceListener`, `KafkaLookupEoriListener` | Spring Kafka retry/DLQ handler devreye girmez |
| 2 | `KafkaTemplate.send()` sonucu beklenmiyor | İki listener | Async producer failure kaybolabilir |
| 3 | DLQ topic/property yok | `application.yml` | Başarısız kayıt korunmaz |
| 4 | Error metadata yok | Listener/config | Root cause ve audit zayıf kalır |
| 5 | DLQ metric/alert yok | `application.yml` / monitoring | Operasyon hatayı proaktif göremez |
| 6 | EORI listener çoklu output üretir | `KafkaLookupEoriListener` | Retry sonrası duplicate/partial success riski var |

---

## Mevcut Topic Yapısı

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

Önemli düzeltme: DLQ, `fdp-sns-input` veya `fdp-sns-lookup-eori` output topic'lerine göre değil, listener'ın tükettiği giriş topic'lerine göre tasarlanmalıdır. Recoverer başarısız olan original consumed record'u DLQ'ye yazar.

---

## Önerilen Konfigürasyon

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

Platform topic provisioning explicit isim gerektiriyorsa bu property'ler environment secret/value olarak verilmelidir; runtime'da string concatenation'a güvenilmemelidir.

---

## Listener Değişikliği

Spring Kafka error handler'ın çalışması için listener hatayı yutmamalıdır. Ayrıca async send hatası listener thread'ine dönmelidir.

### Kaynak Listener

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

EORI listener bir landing kaydından birden fazla output send üretebildiği için tüm send sonuçları beklenmelidir. Partial success ve duplicate riskleri ayrıca değerlendirilmelidir.

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

## Error Handler Tasarımı

Önerilen yaklaşım `DefaultErrorHandler` ve explicit destination resolver kullanmaktır. Default recoverer suffix davranışına güvenmek yerine giriş topic -> DLQ topic mapping'i açık yazılmalıdır.

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

Notlar:

- Spring Boot auto-configuration bu `DefaultErrorHandler` bean'ini listener container factory'ye bağlamazsa mevcut factory üzerinde `setCommonErrorHandler(...)` açıkça çağrılmalıdır.
- DLQ producer serializer'ı hem `GenericRecord` hem `CdlzLandingRecord` payload'larını yazabilmelidir.
- Spring Kafka'nın DLT header'ları kullanılmalı; gerekiyorsa proje özelinde `original-topic`, `original-partition`, `original-offset`, `error-class`, `error-message`, `failed-at` header'ları eklenmelidir.

---

## Metadata Header Önerisi

| Header | Açıklama |
|--------|----------|
| `original-topic` | Hatanın geldiği Kafka topic |
| `original-partition` | Original partition |
| `original-offset` | Original offset |
| `error-class` | Exception class adı |
| `error-message` | Kısa hata mesajı |
| `failed-at` | ISO-8601 timestamp |
| `listener-id` | Hangi listener/container kaynaklı olduğu |
| `delivery-attempt` | Retry deneme sayısı |

Stack trace'i header'a koymak genelde önerilmez; header boyutu büyür. Stack trace loglarda ve tracing sisteminde tutulmalı, DLQ header'da kısa hata özeti kalmalıdır.

---

## Monitoring

Mevcut `application.yml` Dynatrace ve Prometheus endpoint altyapısını içeriyor. Yeni DLQ akışı için aşağıdaki metric'ler eklenmelidir:

| Metric | Boyutlar | Amaç |
|--------|----------|------|
| `fdp.dlq.messages.total` | `topic`, `source_topic`, `exception` | DLQ'ye düşen kayıt sayısı |
| `fdp.kafka.listener.retry.total` | `topic`, `exception` | Retry deneme sayısı |
| `fdp.dlq.publish.failure.total` | `topic`, `exception` | DLQ publish başarısızlığı |

Alert başlangıç önerisi:

| Alert | Koşul | Severity |
|-------|-------|----------|
| DLQ Messages Detected | 5 dakika içinde `fdp.dlq.messages.total > 0` | Warning |
| DLQ Spike | 5 dakika içinde `fdp.dlq.messages.total > 10` | Critical |
| DLQ Publish Failure | `fdp.dlq.publish.failure.total > 0` | Critical |

---

## Test Stratejisi

| Seviye | Test |
|--------|------|
| Unit | Listener exception'ı yutmuyor; send future failure `IllegalStateException` olarak dışarı çıkıyor. |
| Spring context | `DefaultErrorHandler` bean'i yükleniyor ve listener factory'ye bağlanıyor. |
| Serializer | DLQ KafkaTemplate `GenericRecord` ve `CdlzLandingRecord` payload'larını serialize edebiliyor. |
| Integration | Mevcut docker-compose profiline hatalı kayıt senaryosu ekleniyor; retry sonrası kayıt doğru DLQ topic'inde görülüyor. |
| Regression | Başarılı kayıtlar mevcut `fdp-sns-input` ve `fdp-sns-lookup-eori` akışını bozmuyor. |

`TopologyTestDriver` listener seviyesindeki DLQ davranışını test etmek için doğru araç değildir; mevcut Kafka Streams/topology testleri korunmalı, DLQ için listener/integration test eklenmelidir.

---

## Rollback

1. `app.dlq.enabled=false` ile yeni error handler davranışını kapat.
2. Listener değişikliğinin exception propagation etkisini rollback planında ayrıca değerlendir; eski davranışa dönmek veri kaybı riskini geri getirir.
3. DLQ topic'lerini silme; içlerindeki kayıtlar incident/audit için korunmalıdır.

---

## İlgili Belgeler

- [DLQ Neden Yapılmalı](00-dlq-neden-yapilmali.md)
- [Uygulama Kılavuzu](02-uygulama-kilavuzu.md)
- [Runbook](03-runbook.md)
