# Kafka Retry ve DLQ Uygulama Kılavuzu

---

## Ön Koşullar

- DLQ topic isimleri platform ekibiyle netleştirilmeli.
- DLQ producer'ın Avro serializer ayarları `GenericRecord` ve `CdlzLandingRecord` için doğrulanmalı.
- Listener error handler'ın mevcut `fdp-commons` Kafka configuration tarafından ezilmediği kontrol edilmeli.
- Retryable ve non-retryable exception listesi ekipçe onaylanmalı.
- Başarısız EORI landing kaydının tekrar işlenmesi duplicate lookup kayıtları üretebilir; key/idempotency yaklaşımı onaylanmalı.
- **DLQ'nün hangi Kafka cluster'ında olacağı netleştirilmeli.** Listener'lar source topic'leri CDLZ cluster'ından (`app.cdlz-kafka`, `FDP_APP_CDL_KAFKA_BROKER`, group `cdlz-sns`) tüketir; bu, output topic'lerin kullandığı adaptör cluster'ından (`app.kafka`, `FDP_KAFKA_BROKER`) farklı bir cluster'dır. `DeadLetterPublishingRecoverer` original consumed record'u yeniden yazdığı için DLQ topic'leri ve DLQ `KafkaTemplate`'i varsayılan adaptör producer'ı değil, **CDLZ** cluster bootstrap server'ları ve schema registry'sini kullanmalıdır.
- **Consumer'ın `ErrorHandlingDeserializer` kullandığı doğrulanmalı.** Deserialization ve uyumsuz schema hataları listener çalışmadan önce, consumer poll sırasında oluşur. CDLZ consumer factory (`fdp-commons` tarafından yönetilir) key/value Avro deserializer'larını `ErrorHandlingDeserializer` ile sarmadığı sürece bu hatalar container'ı bloke eder ve `DefaultErrorHandler`'a ya da DLQ'ye hiç ulaşmaz; oysa bunlar aşağıda ana non-retryable DLQ senaryosu olarak listelenmiştir. `fdp-commons` içinde doğrulanmalıdır.

---

## Fazlara Göre Uygulama Sırası

Bu kılavuzun ana uygulama kapsamı **Faz 1: No Silent Loss SNS Pilot** içindir. Faz 0 discovery tamamlanmadan Faz 1'e başlanmamalıdır.

| Faz | Bu kılavuzdaki karşılığı |
|-----|--------------------------|
| Faz 0: Discovery | Ön koşullar, cluster kararı, deserializer doğrulaması, EORI idempotency kararı |
| Faz 1: No Silent Loss | Konfigürasyon, error handler, kısa blocking retry, listener değişikliği, topic provisioning, temel testler |
| Faz 2: Operational Hardening | Monitoring, alert, runbook ve DLQ inspect prosedürü |
| Faz 3: Retry Topic Pattern | Bu kılavuzda uygulanmaz; lag/downstream outage kanıtı sonrası ayrı tasarlanır |
| Faz 4: Controlled Reprocessing | Bu kılavuzda uygulanmaz; dry-run/audit/RBAC/rate limit olmadan başlanmaz |
| Faz 5: Platform Standard | SNS pilot sonucu `fdp-commons` veya shared template'e taşınırken ele alınır |

Özellikle retry topic ve reprocessor Faz 1'e eklenmemelidir. İlk kazanım, mesajın sessizce kaybolmamasıdır.

---

## 1. Konfigürasyon Ekle

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

Bu örnek mapping mantığını gösterir. Uygulamada `sourceInput` ve `eoriInput` mevcut `app.topic.cdlz-incoming` ve `app.lookup.eori.cdlz-incoming` değerlerinden türetilebilir ya da property olarak açıkça verilebilir.

---

## 3. Error Handler Ekle

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

        // DLQ kaydını source partition'a sabitleme: DLQ topic source'tan daha az
        // partition'a sahip olabilir ve olmayan bir partition'a göndermek hata verir.
        // partition -1 vererek producer partitioner'ın seçmesini sağla. record.partition()
        // yalnızca DLQ partition sayısı >= source partition ise sabitlenmelidir.
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

Eğer mevcut Kafka listener container factory bu bean'i otomatik kullanmıyorsa ilgili factory config içinde `setCommonErrorHandler(kafkaErrorHandler)` çağrısı eklenmelidir.

`@ConditionalOnProperty` bean'i `app.dlq.enabled`'e bağlar. Değer `false` olduğunda bean oluşturulmaz ve container Spring'in varsayılan error handling davranışına döner. `dlqKafkaTemplate`, **CDLZ** cluster'ına göre yapılandırılmış bir `KafkaTemplate` olmalıdır (bkz. Ön Koşullar); çünkü tüketilen kayıtlar ve DLQ topic'leri o cluster'da bulunur.

---

## 4. Retry Politikasını Netleştir

İlk fazda `mode=blocking` önerilir. Bu, aynı consumer partition'ı üzerinde kısa süreli retry yapar ve retry bittiğinde recoverer kaydı DLQ'ye yazar.

Kural:

- Retryable: geçici Kafka, network, schema registry erişim ve timeout hataları.
- Non-retryable: deserialization, uyumsuz schema, kalıcı payload/business validation hataları.
- Uzun bekleme gerekiyorsa blocking retry'ı büyütme; Spring Kafka retry topic pattern'i için ayrı topic ve ACL tasarla.

Bu ayrım yapılmazsa poison message aynı partition'ı tekrar tekrar bekletebilir.

---

## 5. Listener'ları Güncelle

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

## 6. Topic Provisioning

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

## 7. Monitoring

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
| Retry Spike | Retry denemelerinde ani artış | Warning |
| DLQ Spike | 5 dakikada 10+ DLQ mesajı | Critical |
| DLQ Publish Failure | DLQ publish başarısızlığı | Critical |

---

## 8. Testler

### Unit Testler

- `KafkaSourceListener` producer failure aldığında exception fırlatmalı.
- `KafkaLookupEoriListener` herhangi bir send future failure aldığında exception fırlatmalı.
- Interrupted durumda thread interrupt flag geri set edilmeli.
- Retryable exception'da beklenen retry sayısı uygulanmalı.
- Non-retryable exception'da retry beklenmeden DLQ fast-path çalışmalı.
- DLQ topic resolver `landing-1 -> landing-1-dlq` ve `landing-413 -> landing-413-dlq` mapping'ini doğru yapmalı.

### Entegrasyon Testi

Mevcut docker-compose profiline bir failure senaryosu eklenmelidir:

1. Test input topic'e hatalı/işlenemeyen kayıt gönder.
2. Retry denemelerinin tamamlanmasını bekle.
3. Kaydın ilgili DLQ topic'e yazıldığını doğrula.
4. Başarılı kayıtların normal output topic'lere gitmeye devam ettiğini doğrula.

---

## 9. Rollback

```yaml
app:
  dlq:
    enabled: false
```

`enabled: false` değeri `@ConditionalOnProperty` sayesinde `DlqConfig` bean'ini devre dışı bırakır; böylece özel error handler ve DLQ recoverer çalışmaz. Bu, listener'daki exception-propagation değişikliğini geri almaz; error handler kalktığında container'ın varsayılan davranışı devreye girer ve listener'ları eski "yut ve logla" haline döndürmek orijinal veri kaybı riskini geri getirir. Rollback planında her iki parça da ayrıca kararlaştırılmalıdır.

Rollback sırasında DLQ topic'leri silinmemelidir. İçerik, incident analizi ve veri kurtarma için korunmalıdır.

---

## İlgili Belgeler

- [DLQ Neden Yapılmalı](00-dlq-neden-yapilmali.md)
- [Teknik Analiz](01-teknik-analiz.md)
- [Runbook](03-runbook.md)
