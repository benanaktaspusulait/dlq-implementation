# Kafka Retry ve DLQ Discovery Önerisi

| Alan | Değer |
|------|-------|
| Sahip | Benan Aktas |
| Durum | İncelemede |
| Oluşturulma | 2026-07-08 |
| Son güncelleme | 2026-07-09 |
| Kapsam | `cmd-adaptor-sns` modülü |
| Etiketler | kafka, retry, dlq, error-handling, no-silent-loss, data-integrity |

---

## Executive Summary

- **Mevcut risk:** `cmd-adaptor-sns` listener'larında `try/catch + log.error` pattern'i var; bu, başarısız kayıtların başarılı tüketilmiş gibi commit edilmesine yol açabilir.
- **Mevcut risk:** `KafkaTemplate.send()` async çalışıyor; send sonucu beklenmediğinde producer acknowledgement hataları listener'a dönmeyebilir.
- **Mevcut boşluk:** Recover edilebilir failed-record destination yok; retry/DLQ telemetry ve alert ownership tanımlı değil.
- **Öneri:** Kod değişikliğine geçmeden önce Faz 0 discovery zorunlu tamamlanmalı; offset commit, cluster, serializer, deserializer, exception taxonomy ve operasyon sahipliği doğrulanmalıdır.
- **İlk teslim hedefi:** Tam platform standardizasyonu değil, **No Silent Loss SNS Pilot** olmalıdır.
- **Kapsam uyarısı:** Bu bulgular `cmd-adaptor-sns` için doğrulanmıştır; diğer FDP adaptörleri ayrıca validate edilmeden rollout kapsamına alınmamalıdır.

## Ana Mesaj

1. `cmd-adaptor-sns` içinde Kafka listener hataları uygulama kodunda yakalanıp sadece loglanıyor.
2. Bu davranış Spring Kafka retry/error handler zincirini devre dışı bırakıyor; transient hatalar tekrar denenmiyor.
3. Asıl mimari risk **silent loss / silent failure** riskidir; DLQ tek başına çözüm değil, failure-management tasarımının bir parçasıdır.
4. Öneri; listener exception propagation, producer send acknowledgement, bounded retry, exception classification, DLQ, metrics, alerting ve daha sonra controlled replay adımlarını birlikte ele alır.

---

## İncelenen Kanıtlar

| Kaynak | Bulgular |
|--------|----------|
| `cmd-adaptor-sns/src/main/java/uk/gov/ho/dacc/fdp/listener/KafkaSourceListener.java` | `@KafkaListener` içinde `try/catch` var; exception sadece `log.error` ile kapanıyor. |
| `cmd-adaptor-sns/src/main/java/uk/gov/ho/dacc/fdp/listener/KafkaLookupEoriListener.java` | Aynı pattern EORI lookup listener'da da var. Ayrıca bir landing kaydı birden fazla async producer send'i üretiyor. |
| `cmd-adaptor-sns/src/main/resources/application.yml` | Giriş topic'leri `landing-1` ve `landing-413`; DLQ topic/property tanımı yok. |
| `cmd-adaptor-sns/src/main/resources/application.yml` | Dynatrace/Prometheus altyapısı var, fakat DLQ/retry metric'i yok. |
| `cmd-adaptor-sns-integration-tests/pom.xml` | Entegrasyon testi docker-compose profilleriyle çalışıyor; Testcontainers bağımlılığı görünmüyor. |

> **Not:** Bu dokümandaki kod referansları FDP deposu içindeki `cmd-adaptor-sns` modülünü göstermektedir. Yerel checkout yapınıza göre yolları ayarlayın.

Bu belge SNS adaptörü için doğrulanmış bulgulara dayanır. Diğer FDP adaptörleri aynı pattern'i kullanıyor olabilir, fakat bu dokümanda ayrıca doğrulanmadıkları için rollout kapsamına "validasyon sonrası" alınmalıdır.

---

## Mevcut Durum

`cmd-adaptor-sns` iki ana Kafka girişini tüketiyor: CDLZ SNS landing akışı ve EORI landing akışı. Her iki listener da işleme sırasında oluşan hatayı yakalayıp logluyor. Exception listener metodundan dışarı çıkmadığı için Spring Kafka'nın retry ve `DefaultErrorHandler` davranışı devreye giremiyor. Ayrıca `KafkaTemplate.send()` async çalıştığı için producer ack hataları beklenmezse listener başarılı tamamlanmış gibi davranabilir.

---

## Üst Gözlemler

| # | Gözlem | Etki | Öneri | Güven |
|---|--------|------|-------|-------|
| 1 | Listener seviyesinde `try/catch + log.error` var | Başarısız kayıt commit edilebilir ve kaybolabilir | Hataları yutma; Spring Kafka error handler'a bırak | Yüksek |
| 2 | Kafka retry politikası yok | Geçici hatalar doğrudan kayıp/manuel recovery riskine döner | Retryable ve non-retryable exception ayrımı yap | Yüksek |
| 3 | Producer send sonucu beklenmiyor | Async send hataları retry/DLQ akışına girmeyebilir | Send future'larını bekle veya hatayı listener thread'ine taşı | Yüksek |
| 4 | DLQ topic/property yok | Maksimum retry sonrası başarısız kayıtlar geri kazanılamaz | Tüketilen giriş topic'leri için DLQ tanımla | Yüksek |
| 5 | DLQ/retry metric ve alert yok | Hata ancak log aramasıyla bulunur | DLQ ve retry metric'leri ekle | Yüksek |
| 6 | Reprocessing akışı yok | Recovery manuel ve riskli kalır | İlk fazda manuel prosedür, sonraki fazda kontrollü reprocessor | Orta |

---

## Neden DLQ?

### Veri Kaybını Önleme

Bugünkü akış:

```text
Kafka kaydı -> Listener -> Hata -> log.error -> kayıt başarıyla tüketilmiş gibi kapanabilir
```

Hedef akış:

```text
Kafka kaydı -> Listener -> Hata dışarı fırlatılır -> Kafka retry -> hala başarısızsa DLQ -> metric/alert
```

DLQ, hatalı kaydı orijinal payload ve hata metadata'sı ile korumaya yardımcı olur. Ancak DLQ tek başına retry, replay, idempotency veya operasyon sahipliği problemlerini çözmez; kayıt yalnızca kontrollü, onaylı prosedür altında potansiyel olarak tekrar işlenebilir hale gelir.

### Kafka Retry Neden Ayrı Tasarlanmalı?

DLQ son çaredir; transient hatalar önce Kafka consumer retry ile çözülmeye çalışılmalıdır. Örneğin kısa süreli broker, schema registry, network veya downstream erişim problemleri birkaç retry içinde toparlanabilir. Buna karşılık bozuk payload, schema uyumsuzluğu veya kalıcı business validation hataları tekrar denenmemeli, hızlıca DLQ'ye alınmalıdır.

### Operasyonel Görünürlük

| Alan | Bugün | DLQ ile |
|------|-------|---------|
| Hata keşfi | Log araması ve manuel fark etme | Metric ve alert |
| Kayıp kayıt sayısı | Bilinmiyor | DLQ count ile ölçülebilir |
| Root cause analizi | Stack trace/log bağımlı | Header metadata + payload |
| Recovery | Kaynak sistemden yeniden gönderim | DLQ replay/reprocess |

### Audit ve Soruşturma

DLQ; original topic, partition, offset, timestamp, exception type ve error message gibi alanlarla tutulursa "hangi kayıt neden işlenemedi?" sorusu log retention'a bağlı kalmadan cevaplanabilir.

---

## Önerilen Çözüm

**Birincil öneri:** Faz 0 discovery tamamlandıktan sonra kontrollü pilot olarak Spring Kafka `DefaultErrorHandler` ile sınırlı consumer retry + `DeadLetterPublishingRecoverer` ile DLQ + zenginleştirilmiş metadata.

Bu seçenek custom bir hata altyapısı yazmadan Spring Kafka'nın listener retry ve recoverer davranışını kullanır. Proje özelinde kritik nokta, mevcut listener `try/catch` bloklarının hatayı yutmaması ve producer send hatalarının listener thread'ine taşınmasıdır.

### Retry Politikası

| Hata tipi | Aksiyon | Gerekçe |
|-----------|---------|---------|
| Geçici Kafka/Schema Registry/network hatası | Exponential backoff ile retry | Kısa süreli kesintiler DLQ üretmeden toparlanabilir |
| Serialization/schema uyumsuzluğu | Retry yapmadan veya az retry sonrası DLQ | Aynı payload tekrar denenince düzelmez |
| Kalıcı business validation hatası | DLQ | Veri veya kod düzeltmesi gerekir |
| Uzun süreli downstream kesinti | Retry topic pattern'i değerlendir | Uzun blocking retry partition'ı gereksiz bekletebilir |

İlk pilot için kısa blocking retry önerilir. Backoff toplamı dakikaları aşacaksa Spring Kafka retry topic pattern'i (`retry-1m`, `retry-5m`, sonrasında DLQ gibi) ayrı bir sonraki faz olarak tasarlanmalıdır.

### DLQ Topic Kuralı

DLQ topic'i output topic'e değil, listener'ın tükettiği giriş topic'ine bağlanmalıdır:

| Listener | Tüketilen topic property | Local default | Önerilen DLQ |
|----------|--------------------------|---------------|--------------|
| `KafkaSourceListener` | `app.topic.cdlz-incoming` | `landing-1` | `${app.topic.cdlz-incoming}-dlq` |
| `KafkaLookupEoriListener` | `app.lookup.eori.cdlz-incoming` | `landing-413` | `${app.lookup.eori.cdlz-incoming}-dlq` |

Eğer platform topic isimlerinde suffix kullanımına izin vermiyorsa `app.dlq.topics.source` ve `app.dlq.topics.eori` explicit property olarak verilmelidir.

Her iki tüketilen topic de **CDLZ cluster**'ında (`app.cdlz-kafka`, `FDP_APP_CDL_KAFKA_BROKER`) bulunur; bu, adaptörün kendi `app.kafka` cluster'ından (`FDP_KAFKA_BROKER`) farklıdır. Yukarıdaki DLQ topic'leri ve `DeadLetterPublishingRecoverer`'ın kullandığı `KafkaTemplate`, adaptör cluster'ına değil CDLZ cluster'ına göre provizyonlanmalı ve yapılandırılmalıdır.

---

## Çözüm Seçenekleri

| Seçenek | Açıklama | Artı | Eksi |
|---------|----------|------|------|
| A | Blocking retry + `DefaultErrorHandler` + DLQ | En düşük karmaşıklıktaki pilot seçeneği | Uzun kesintilerde partition bekleyebilir |
| B | Blocking retry + explicit DLQ resolver + metadata + metric | Önerilen ilk faz; audit ve alert destekler | Retry sınıflandırması/test gerekir |
| C | Retry topic pattern + DLQ | Uzun backoff ve yüksek hacim için güçlü | Daha fazla topic ve operasyonel karmaşıklık |

---

## Fazlı Yol Haritası

Bu iş tek seferde "büyük Kafka platform değişikliği" olarak yapılmamalıdır. En güvenli sıra: önce davranışı anlamak, sonra sessiz kaybı bitirmek, sonra operasyon ve replay kabiliyetini kontrollü şekilde büyütmektir.

### Faz 0: Architecture Discovery

Bu faz hard gate'tir. Aşağıdaki kararlar dokümante edilmeden Faz 1 implementasyonuna başlanmamalıdır.

Davranış değiştirmeden kanıt topla.

- [ ] Offset commit semantiğini doğrula: başarı, retry exhausted, DLQ publish success/failure durumlarında offset ne oluyor?
- [ ] CDLZ cluster vs adaptor cluster ayrımını netleştir; DLQ topic ve `KafkaTemplate` hangi cluster'da olacak?
- [ ] CDLZ consumer factory'nin `ErrorHandlingDeserializer` kullanıp kullanmadığını doğrula.
- [ ] EORI akışında partial success ve duplicate etkisini analiz et.
- [ ] Retryable/non-retryable exception taxonomy'sini yaz.

#### Phase Gate / Decision Checklist

Faz Gate'in tam checklist'ini [Teknik Analiz](01-teknik-analiz.md#phase-gate--decision-checklist) belgesinde bulabilirsiniz. Gate bir duraklama noktasıdır; tüm maddeler doğrulanmadan Faz 1 başlamamalıdır.

Çıkış kriteri: error taxonomy, offset commit kararı, DLQ cluster kararı ve EORI idempotency kararı dokümante edilmiş olmalı.

### Faz 1: No Silent Loss SNS Pilot

Amaç sadece sessiz veri kaybını durdurmaktır; retry topic ve reprocessor bu faza dahil edilmemelidir.

- [ ] `try/catch + log.error` pattern'ini kaldır veya exception'ı rethrow et.
- [ ] Producer send future'larını bekleyerek async send hatalarını görünür yap.
- [ ] Kısa blocking retry ekle: max retry, exponential backoff, non-retryable fast-path.
- [ ] Retry exhausted sonrası original consumed record'u DLQ'ye gönder.
- [ ] DLQ publish failure için critical metric/alert ekle.
- [ ] Unit ve docker-compose entegrasyon testleriyle silent loss'un bittiğini doğrula.
- [ ] EORI, Faz 1'e ancak idempotency/duplicate stratejisi onaylandıysa dahil edilsin; aksi halde SNS pilot dışında tasarım konusu olarak kalsın.

Çıkış kriteri: kayıt ya başarıyla işlenir ya retry edilir ya da DLQ'ye korunarak gider; `log.error` ile sessiz kayıp kalmaz.

### Faz 2: Operational Hardening

Sistemi production'da işletilebilir hale getir.

- [ ] Retry count, retry exhausted, DLQ count, DLQ publish failure metric'lerini dashboard'a ekle.
- [ ] Consumer lag ve retry spike alert'lerini tanımla.
- [ ] Runbook'u gerçek topic, cluster, ACL ve serializer kontrolleriyle tamamla.
- [ ] DLQ inceleme prosedürünü ve incident kaydı formatını netleştir.
- [ ] Schema evolution ve schema mismatch kararlarını runbook'a bağla.

Çıkış kriteri: ekip bir DLQ/retry olayını 5 dakika içinde fark edip doğru prosedürü izleyebilmeli.

### Faz 3: Retry Topic Pattern

Bunu yalnızca evidence varsa ekle.

- [ ] Blocking retry partition lag yaratıyor mu ölç.
- [ ] Downstream kesintilerinin dakika seviyesinde bekleme gerektirip gerektirmediğini doğrula.
- [ ] Gerekirse `retry-1m`, `retry-5m`, `retry-30m`, final DLQ topic zincirini tasarla.
- [ ] Her retry topic için ACL, retention, monitoring ve ownership tanımla.

Çıkış kriteri: retry topic pattern'i yalnızca blocking retry'nin yetmediği kanıtlanırsa uygulanır.

### Faz 4: Controlled Reprocessing

Replay otomasyonu en son gelmelidir; yanlış replay ikinci incident yaratabilir.

- [ ] Dry-run, offset range, rate limit, audit log ve yetki kontrolü olmadan reprocessor yazma.
- [ ] Replay reason, actor, timestamp ve replay sonucu audit trail'e yazılmalı.
- [ ] EORI duplicate etkisi çözülmeden bulk replay açılmamalı.

Çıkış kriteri: replay kontrollü, izlenebilir ve geri dönüş etkisi analiz edilmiş şekilde yapılabilir.

### Faz 5: Platform Standardization

SNS pilot başarılı olduktan sonra pattern ortak hale getirilebilir.

- [ ] Ortak retry/DLQ config modelini `fdp-commons` veya shared template olarak tasarla.
- [ ] Ortak metric, header, runbook ve dashboard standardı oluştur.
- [ ] Diğer adaptörleri aynı taxonomy ve phase gate'lerle onboard et.
- [ ] Schema compatibility ve DLQ governance'ı platform standardına ekle.

Çıkış kriteri: SNS'e özel çözüm değil, FDP adaptörleri için tekrar edilebilir failure-management standardı oluşur.

---

## Ana Riskler

| Risk | Etki | Azaltma |
|------|------|---------|
| Listener exception'ı yutmaya devam eder | Error handler çalışmaz | Try/catch kaldırma/rethrow için test yaz |
| Yanlış hata retry edilir | Poison message aynı partition'ı gereksiz bekletir | Non-retryable exception listesi ve DLQ fast-path ekle |
| Retry backoff çok uzun olur | Consumer partition'ı bloklanır, lag artar | Kısa blocking retry; uzun bekleme için retry topic pattern'i |
| Async send hatası listener'a dönmez | Producer hatası DLQ'ye düşmez | Send future'ını bekle veya callback hatasını kontrollü taşı |
| DLQ serializer yanlış olur | DLQ publish de başarısız olur | DLQ için Avro serializer entegrasyon testi ekle |
| EORI partial output oluşur | Retry duplicate kayıt üretebilir | Idempotent key, transactional send veya duplicate handling stratejisi belirle |
| DLQ topic yanlış isimlenir | Operasyon topic'i bulamaz | Tüketilen topic bazlı explicit mapping kullan |
| DLQ yanlış cluster'a yazılır | CDLZ cluster'ından tüketilen kayıtlar yeniden yayınlanamaz/bulunamaz | DLQ topic'lerini ve DLQ `KafkaTemplate`'ini CDLZ cluster'ına (`FDP_APP_CDL_KAFKA_BROKER`) yönlendir, adaptör cluster'ına değil |
| Deserialization hataları handler'ı atlar | Poison/schema hataları poll döngüsünü kırar, DLQ'ye hiç ulaşmaz | Consumer Avro deserializer'larını `ErrorHandlingDeserializer` ile sar (`fdp-commons` içinde) |
| `app.dlq.enabled` flag'i bir bean'e bağlı değil | Flag üzerinden rollback hiçbir etki yaratmaz | `DlqConfig`'i `app.dlq.enabled` üzerinde `@ConditionalOnProperty` ile koşullandır |

---

## Architecture Decision Records to Create

| ADR | Karar |
|-----|-------|
| ADR-001 | DLQ topic'leri output topic'lere değil, tüketilen source topic'lere aittir. |
| ADR-002 | DLQ topic'leri ve DLQ producer CDLZ cluster'ını kullanır. |
| ADR-003 | Faz 1 için yalnızca kısa blocking retry kullanılır. |
| ADR-004 | Retry topic'ler lag/downstream outage kanıtı oluşana kadar ertelenir. |
| ADR-005 | Dry-run, RBAC, audit, rate limit ve duplicate handling olmadan automated replay yapılmaz. |
| ADR-006 | EORI replay, idempotency/duplicate stratejisi netleşmeden enable edilmez. |
| ADR-007 | Schema/deserialization DLQ davranışı için `ErrorHandlingDeserializer` doğrulanmalıdır. |

---

## İlgili Belgeler

- [Teknik Analiz](01-teknik-analiz.md)
- [Uygulama Kılavuzu](02-uygulama-kilavuzu.md)
- [Runbook](03-runbook.md)

---

## Sözlük

| Terim | Tanım |
|-------|-------|
| DLQ | Dead Letter Queue: retry sonrası işlenemeyen mesajların depolandığı özel topic |
| CDLZ cluster | `cmd-adaptor-sns` tarafından tüketilen source topic'lerin bulunduğu Kafka cluster'ı (broker: `FDP_APP_CDL_KAFKA_BROKER`) |
| Adaptor cluster | `cmd-adaptor-sns`'in kendi output topic'leri için kullandığı Kafka cluster'ı (broker: `FDP_KAFKA_BROKER`) |
| Blocking retry | Aynı kaydı tekrar denerken partition'ı bloke eden consumer retry |
| Non-retryable | Ek retry olmadan doğrudan DLQ'ye gitmesi gereken exception |
| ErrorHandlingDeserializer | Deserialization hatalarını yakalayan ve error handler'a yönlendiren Kafka deserializer sarmalayıcısı |
| DefaultErrorHandler | Retry ve dead-letter publishing desteği sunan Spring Kafka dahili error handler'ı |
| DeadLetterPublishingRecoverer | Başarısız kayıtları DLQ topic'ine yayınlayan Spring Kafka recoverer'ı |
