# Kafka Failure Handling Discovery Teknik Analizi: `cmd-adaptor-sns`

---

## Kapsam

Bu analiz FDP deposu içindeki SNS command adaptor için hazırlanmıştır. İncelenen ana modül `cmd-adaptor-sns`, ilgili entegrasyon test modülü ise `cmd-adaptor-sns-integration-tests`tir.

> **Not:** Modül yollarını yerel checkout yapınıza göre ayarlayın.

Diğer FDP adaptörleri için aynı öneri ancak kod pattern'i doğrulandıktan sonra genelleştirilmelidir. Bu analiz, immediate coding task değil, **silent loss** riskini azaltmaya yönelik discovery ve kontrollü pilot kararlarını destekler.

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

İki giriş topic'i (`landing-1`, `landing-413`) **CDLZ cluster**'ından (`app.cdlz-kafka`, broker `FDP_APP_CDL_KAFKA_BROKER`, group `cdlz-sns`) tüketilir; bu, output topic'lerin kullandığı adaptörün kendi `app.kafka` cluster'ından (`FDP_KAFKA_BROKER`) farklı bir cluster'dır. Bu cluster ayrımı DLQ tasarımı için kritiktir: DLQ topic'leri ve DLQ producer'ı adaptör cluster'ında değil, CDLZ cluster'ında bulunmalıdır.

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

Platform topic provisioning explicit isim gerektiriyorsa bu property'ler environment secret/value olarak verilmelidir; runtime'da string concatenation'a güvenilmemelidir.

---

## Kafka Retry Tasarımı

Retry, DLQ'den ayrı bir karar noktasıdır. Hedef, transient hataları DLQ üretmeden toparlamak; kalıcı veya bozuk payload hatalarını ise aynı partition'ı bekletmeden DLQ'ye almaktır.

| Retry tipi | Ne zaman kullanılır | Dikkat |
|------------|---------------------|--------|
| Producer retry | Kafka producer'ın broker'a yazma sırasında yaşadığı geçici hatalar | Listener processing retry'nin yerine geçmez; `KafkaTemplate.send()` sonucu yine gözlenmelidir |
| Blocking consumer retry | Kısa süreli transient processing hataları | Partition aynı kayıt üzerinde bekler; toplam backoff kısa tutulmalıdır |
| Retry topic pattern | Dakikalar seviyesinde bekleme veya downstream outage | Ek topic, ACL, retention ve monitoring gerekir |
| DLQ fast-path | Schema/serialization/kalıcı business validation hataları | Aynı kaydı tekrar denemek fayda sağlamaz |

İlk SNS pilotu için öneri blocking consumer retry'dır: örneğin 3 retry, 1 saniye başlangıç, 2x multiplier, 30 saniye maksimum interval. Eğer toplam bekleme süresi operasyonel olarak kabul edilemeyecek kadar uzarsa veya downstream kesintileri dakikalar seviyesinde bekleniyorsa Faz 3'te retry topic pattern'i tasarlanmalıdır.

Exception sınıflandırması açık olmalıdır:

| Sınıf | Örnek | Aksiyon |
|-------|-------|---------|
| Retryable | `TimeoutException`, geçici broker/network hataları, schema registry geçici erişim hatası | Exponential backoff retry |
| Non-retryable | Deserialization, uyumsuz schema, kalıcı payload validation hatası | DLQ fast-path |
| İncelenecek | EORI multi-send partial failure | Idempotency/duplicate stratejisi netleşmeden agresif retry yapma |

---

## Fazlara Göre Mimari Kararlar

| Faz | Mimari karar | Bu fazda yapılmayanlar |
|-----|--------------|------------------------|
| Faz 0: Discovery | Offset commit, CDLZ/adaptor cluster ayrımı, `ErrorHandlingDeserializer`, serializer desteği, EORI duplicate etkisi, exception taxonomy, ownership ve topic provisioning doğrulanır | Runtime davranışı değiştirilmez; Faz 1 başlamaz |
| Faz 1: No Silent Loss | Kısa blocking retry, DLQ, DLQ publish failure alert ve listener exception propagation uygulanır | Retry topic, reprocessor, bulk replay yok |
| Faz 2: Operational Hardening | Dashboard, alert, runbook, DLQ inspect prosedürü ve schema mismatch operasyon kararı tamamlanır | Yeni retry topolojisi eklenmez |
| Faz 3: Retry Topic Pattern | Sadece lag/downstream outage kanıtı varsa retry topic zinciri tasarlanır | Kanıt yoksa blocking retry büyütülmez |
| Faz 4: Controlled Reprocessing | Dry-run, offset range, audit, RBAC ve rate limit içeren replay modeli tasarlanır | Kontrolsüz API veya bulk replay yok |
| Faz 5: Platform Standard | SNS pilotu ortak config/header/metric/runbook standardına dönüştürülür | Diğer adaptörlere doğrulamadan kopyalanmaz |

Bu sıralama, kritik güvenlik kazanımını Faz 1'de alırken daha riskli otomasyonları ölçüm ve operasyon disiplini sonrasına bırakır.

### Phase Gate / Decision Checklist

Faz 0 aşağıdaki checklist tamamlanmadan kapanmış sayılmamalıdır:

- [ ] Listener success, listener exception, retry exhausted, DLQ publish success ve DLQ publish failure durumlarında offset commit semantiği doğrulandı.
- [ ] CDLZ cluster ve adaptor cluster sorumlulukları yazılı hale getirildi.
- [ ] `fdp-commons` içinde `ErrorHandlingDeserializer` varlığı ve schema/deserialization hata davranışı doğrulandı.
- [ ] DLQ `KafkaTemplate`'in **CDLZ** cluster'ını hedefleyeceği doğrulandı.
- [ ] DLQ producer serializer'ının `GenericRecord` ve `CdlzLandingRecord` payload'larını desteklediği test edildi.
- [ ] EORI idempotency, duplicate ve partial-success davranışı karara bağlandı.
- [ ] Retryable/non-retryable exception taxonomy'si ekipçe onaylandı.
- [ ] Alert owner, DLQ triage owner, replay approver ve first-response SLA belirlendi.
- [ ] DLQ topic provisioning, retention, ACL ve schema registry konfigürasyonu onaylandı.

---

## Architecture Decision Records to Create

| ADR | Karar adayı |
|-----|-------------|
| ADR-001 | DLQ topic'leri output topic'lere değil, tüketilen source topic'lere aittir. |
| ADR-002 | DLQ topic'leri ve DLQ producer CDLZ cluster'ını kullanır. |
| ADR-003 | Faz 1 için kısa blocking retry kullanılır. |
| ADR-004 | Retry topic'ler lag/downstream outage kanıtı oluşana kadar ertelenir. |
| ADR-005 | Dry-run, RBAC, audit, rate limit ve duplicate handling olmadan automated replay yapılmaz. |
| ADR-006 | EORI replay için idempotency/duplicate stratejisi prerequisite'tir. |
| ADR-007 | Deserialization/schema hatalarının DLQ davranışı için `ErrorHandlingDeserializer` doğrulanır. |

### ADR Şablonu

Her ADR aşağıdaki yapıyı izlemelidir:

```markdown
# ADR-NNN: <Başlık>

## Durum
Önerildi | Onaylandı | Kullanımdan kaldırıldı | ADR-XXX ile değiştirildi

## Bağlam
Bu kararı tetikleyen sorun nedir?

## Karar
Önerilen değişiklik nedir?

## Sonuçlar
Olumlu ve olumsuz sonuçlar nelerdir?

## Alternatifler
Hangi diğer seçenekler değerlendirildi?
```

ADR'leri depoda `docs/adr/` veya ekibin kararlaştırdığı konumda saklayın.

---

## Listener Değişikliği

Aşağıdaki kodlar Faz 0 kararları doğrulandıktan sonra değerlendirilecek **illustrative candidate implementation** örnekleridir; mevcut `fdp-commons` Kafka factory davranışı ve Spring Kafka versiyonu doğrulanmadan doğrudan uygulanmamalıdır.

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

EORI ayrıca ayrı risk alanıdır:

- Bir consumed landing record birden fazla `fdp-sns-lookup-eori` output kaydı üretebilir.
- Bazı output send'ler başarılı olduktan sonra hata alınırsa original input retry edildiğinde duplicate oluşabilir.
- DLQ'den replay, output key/idempotency/downstream duplicate semantics anlaşılmadan lookup kayıtlarını tekrar üretebilir.
- Bu nedenle SNS source listener, ilk **No Silent Loss** pilot için daha düşük riskli adaydır; EORI Faz 1'e dahil edilecekse idempotency/duplicate stratejisi önce onaylanmalıdır.

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

Bu da Faz 0 sonrası aday implementasyondur. Spring Boot, mevcut `fdp-commons` listener container factory konfigürasyonuna bağlı olarak `DefaultErrorHandler` bean'ini otomatik bağlamayabilir.

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
            // partition -1 producer partitioner'ın geçerli bir partition seçmesini sağlar.
            // record.partition() sabitlemek, DLQ topic source'tan daha az partition'a
            // sahipse hata verir. Yalnızca DLQ partition >= source partition ise sabitle.
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

Notlar:

- Bu bean `app.dlq.enabled`'e bağlanmalıdır (örneğin `@ConditionalOnProperty(prefix = "app.dlq", name = "enabled", havingValue = "true", matchIfMissing = true)`); aksi halde `enabled` property'si ve rollback adımı çalışmaz. Bu olmadan `app.dlq.enabled=false` ayarının runtime'da hiçbir etkisi olmaz.
- Spring Boot auto-configuration bu `DefaultErrorHandler` bean'ini listener container factory'ye bağlamazsa mevcut factory üzerinde `setCommonErrorHandler(...)` açıkça çağrılmalıdır.
- `dlqKafkaTemplate`, schema registry'siyle birlikte **CDLZ** cluster'ını (`app.cdlz-kafka` / `FDP_APP_CDL_KAFKA_BROKER`) hedeflemelidir; çünkü listener'lar o cluster'dan tüketir ve recoverer original consumed record'u yeniden yayınlar. Varsayılan adaptör `KafkaTemplate`'i (`app.kafka` / `FDP_KAFKA_BROKER`) kullanmak DLQ'yü yanlış cluster'a yazar.
- DLQ producer serializer'ı hem `GenericRecord` hem `CdlzLandingRecord` payload'larını yazabilmelidir.
- `addNotRetryableExceptions(DeserializationException.class, ...)` yalnızca consumer factory Avro deserializer'larını `ErrorHandlingDeserializer` ile sararsa işe yarar. Aksi halde deserialization/schema hataları poll döngüsünü kırar ve bu handler'a ya da DLQ'ye hiç ulaşmaz. `fdp-commons` içinde doğrulanmalıdır.
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
| `fdp.kafka.listener.retry.exhausted.total` | `topic`, `exception` | Retry limiti sonrası DLQ'ye giden kayıt sayısı |
| `fdp.dlq.publish.failure.total` | `topic`, `exception` | DLQ publish başarısızlığı |

Alert başlangıç önerisi:

| Alert | Koşul | Severity |
|-------|-------|----------|
| DLQ Messages Detected | 5 dakika içinde `fdp.dlq.messages.total > 0` | Warning |
| DLQ Spike | 5 dakika içinde `fdp.dlq.messages.total > 10` | Critical |
| DLQ Publish Failure | `fdp.dlq.publish.failure.total > 0` | Critical |

Operasyonel sahiplik de metric kadar önemlidir:

| Soru | Faz 0/Faz 2 kararı |
|------|--------------------|
| Alert'i kim sahiplenir? | On-call uygulama ekibi mi, platform/DevOps mu yazılmalı |
| DLQ kayıtlarını kim triage eder? | Uygulama owner + gerekirse schema/platform owner |
| Replay'i kim onaylar? | Service owner veya incident commander |
| İlk yanıt SLA'i nedir? | Örn. warning için 30 dk, critical için 5 dk gibi netleştirilmeli |
| Dashboard nerede? | Retry count, retry exhausted, DLQ count, DLQ publish failure, source topic lag ve consumer lag aynı görünümde olmalı |

`DLQ Messages Detected` otomatik replay tetiklememelidir; triage başlatmalıdır.

---

## Test Stratejisi

| Seviye | Test |
|--------|------|
| Unit | Listener exception'ı yutmuyor; send future failure `IllegalStateException` olarak dışarı çıkıyor. |
| Spring context | `DefaultErrorHandler` bean'i yükleniyor ve listener factory'ye gerçekten bağlanıyor. |
| Feature flag | `app.dlq.enabled=false` custom DLQ recoverer kullanımını engelliyor. |
| Retry policy | Retryable exception'larda retry sayısı ve backoff uygulanıyor; non-retryable exception'larda DLQ fast-path çalışıyor. |
| Serializer | DLQ KafkaTemplate `GenericRecord` ve `CdlzLandingRecord` payload'larını serialize edebiliyor. |
| Deserializer | `ErrorHandlingDeserializer` varken ve yokken deserialization failure davranışı doğrulanıyor. |
| DLQ publish failure | DLQ publish hatası critical metric'i artırıyor ve görünür fail ediyor. |
| Misconfiguration | Yanlış DLQ topic/cluster konfigürasyonu sessiz kalmıyor, görünür fail ediyor. |
| EORI partial send | Partial-send failure ve duplicate riski testle görünür hale getiriliyor. |
| Rollback | `app.dlq.enabled=false` ve listener exception propagation etkisinin rollback davranışı anlaşılıyor. |
| Integration | Mevcut docker-compose profiline hatalı kayıt senaryosu ekleniyor; retry sonrası kayıt doğru DLQ topic'inde görülüyor. |
| Regression | Başarılı kayıtlar mevcut `fdp-sns-input` ve `fdp-sns-lookup-eori` akışını bozmuyor. |

`TopologyTestDriver` listener seviyesindeki DLQ davranışını test etmek için doğru araç değildir; mevcut Kafka Streams/topology testleri korunmalı, DLQ için listener/integration test eklenmelidir.

---

## Rollback

1. `app.dlq.enabled=false` ile yeni error handler davranışını kapat.
2. Listener değişikliğinin exception propagation etkisini rollback planında ayrıca değerlendir; eski davranışa dönmek veri kaybı riskini geri getirir.
3. DLQ topic'lerini silme; içlerindeki kayıtlar incident/audit için korunmalıdır.

---

## Güvenlik Hususları

DLQ kayıtları orijinal mesaj payload'ını içerir; hassas veri içerebilir. Aşağıdakiler ele alınmalıdır:

| Alan | Gereklilik |
|------|------------|
| Veri sınıflandırması | DLQ payload'larında hangi PII/hassas alanların bulunabileceği doğrulanmalı |
| Rest'te şifreleme | DLQ topic'leri, source topic'ler ile aynı rest'te şifreleme politikasını kullanmalıdır |
| Erişim kontrolü | DLQ topic ACL'leri yalnızca şunlara kısıtlı olmalıdır: uygulama (produce), triage/operasyon (consume), reprocessor servisi (consume) |
| Retention uyumu | DLQ retention, veri saklama politikasıyla uyumlu olmalı; hassas veri gerekenden uzun süre tutulmamalıdır |
| Audit trail | DLQ topic'lerine console-consumer erişimi audit için loglanmalıdır |

---

## Felaket Kurtarma

| Senaryo | Etki | Azaltma |
|---------|------|---------|
| DLQ topic kullanılamaz hale gelir | DLQ publish başarısız olur; `fdp.dlq.publish.failure.total` alert'i tetiklenir | DLQ publish failure'ı kritik olarak izle; production ortamında topic replication factor ≥ 2 olmasını sağla |
| CDLZ cluster kesintisi | Consumer okuyamaz; DLQ'ye yazamaz | Bu kaynak cluster sorunudur, DLQ'ye özgü değil; CDLZ incident prosedürünü takip et |
| Schema registry kesintisi | Deserialization hataları artar; DLQ fast-path'e düşebilir | Schema registry'nin HA konfigürasyonu olduğundan emin ol; `SerializationException` hızını izle |

---

## Kapasite Planlama

| Faktör | Rehber |
|--------|--------|
| DLQ partition sayısı | Source topic partition sayısıyla eşleşmeli; recoverer'da partition `-1` kullanarak uyumsuzluk hatalarını önle |
| DLQ retention | Incident inceleme süresini karşılayacak şekilde ayarla (≥ 7 gün önerilir); veri saklama politikasıyla uyumlu olmalı |
| DLQ depolama büyümesi | `fdp.dlq.messages.total` izleyerek depolama ihtiyacını öngör; sürdürülen DLQ artışında uyarı ver |
| Beklenen DLQ hızı | Steady state'te DLQ count 0 olmalıdır; sürekli DLQ mesajları sistematik bir sorun olduğunu gösterir |

---

## İlgili Belgeler

- [Kafka Retry ve DLQ Discovery Önerisi](00-dlq-neden-yapilmali.md)
- [Uygulama Kılavuzu](02-uygulama-kilavuzu.md)
- [Runbook](03-runbook.md)
