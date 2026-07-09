# Kafka Retry ve DLQ Operasyonel Runbook

---

## Durum

Bu runbook, Kafka retry ve DLQ implementasyonu devreye alındıktan sonra kullanılacak operasyon prosedürünü tarif eder. Reprocessing API henüz geliştirilmediyse API endpoint'leri varmış gibi kabul edilmemelidir; manuel replay yalnızca onaylı operasyon prosedürüyle yapılmalıdır.

---

## Hızlı Referans

| Durum | İlk Aksiyon | Öncelik |
|-------|-------------|---------|
| DLQ'de mesaj var | Topic, exception ve deploy zamanını kontrol et | Yüksek |
| Retry spike var | Exception tipini ve source topic lag'ini kontrol et | Yüksek |
| Retry exhausted var | Maksimum retry sonrası DLQ'ye giden kayıtları incele | Yüksek |
| DLQ publish failure var | Kafka ACL/serializer/topic varlığını kontrol et | Kritik |
| Retry artıyor ama DLQ yok | Listener error handler ve retry config'i kontrol et | Yüksek |
| Reprocess edilen kayıt tekrar DLQ'ye düşüyor | Root cause giderilmeden replay'i durdur | Yüksek |

Hızlı kurallar:

- Root cause düzeltilmeden replay yapılmaz.
- İnceleme sırasında DLQ mesajları silinmez.
- Duplicate etkisi anlaşılmadan EORI kayıtları bulk replay edilmez.
- Production console-consumer kullanımı erişim, audit ve veri hassasiyeti kurallarına uymalıdır.

---

## Fazlara Göre Operasyon Modeli

| Faz | Operasyon beklentisi |
|-----|----------------------|
| Faz 0: Discovery | Runbook sadece kararları kaydeder; production davranışı değişmez |
| Faz 1: No Silent Loss | Retry/DLQ alertleri izlenir; replay otomasyonu yoktur, DLQ kayıtları silinmez |
| Faz 2: Operational Hardening | Dashboard, alert threshold, DLQ inspect ve incident prosedürü aktif kullanılır |
| Faz 3: Retry Topic Pattern | Retry topic lag, retention ve ownership ayrıca izlenir |
| Faz 4: Controlled Reprocessing | Replay yalnızca dry-run, onay, audit ve rate limit ile yapılır |
| Faz 5: Platform Standard | Aynı runbook pattern'i diğer adaptörlere standardize edilir |

Faz 1'de bir DLQ mesajı oluşursa doğru aksiyon hemen replay yapmak değildir; önce root cause, duplicate etkisi ve schema/serializer durumu doğrulanmalıdır.

---

## DLQ Topic'leri

| Akış | Source topic | DLQ topic |
|------|--------------|-----------|
| SNS landing | `${app.topic.cdlz-incoming}` / local `landing-1` | `landing-1-dlq` veya explicit `FDP_CMD_ADAPTOR_SOURCE_DLQ_TOPIC` |
| EORI landing | `${app.lookup.eori.cdlz-incoming}` / local `landing-413` | `landing-413-dlq` veya explicit `FDP_CMD_ADAPTOR_EORI_DLQ_TOPIC` |

---

## Monitoring

### Metric'ler

| Metric | Normal | Uyarı | Kritik |
|--------|--------|-------|--------|
| `fdp.dlq.messages.total` | 0 | 5 dakikada > 0 | 5 dakikada > 10 |
| `fdp.kafka.listener.retry.total` | Düşük/değişken | Ani artış | Sürekli artış |
| `fdp.kafka.listener.retry.exhausted.total` | 0 | 5 dakikada > 0 | 5 dakikada > 10 |
| `fdp.dlq.publish.failure.total` | 0 | > 0 | > 0 |

### İlk Kontroller

1. DLQ artışı hangi source topic'ten geliyor?
2. Exception tipi nedir?
3. Son deploy veya config değişikliği var mı?
4. Schema Registry, Kafka ACL ve downstream servislerde hata var mı?
5. EORI akışında duplicate/partial output ihtimali var mı?

### Operasyonel Sahiplik

| Soru | Runbook kararı |
|------|----------------|
| Alert'i kim sahiplenir? | On-call uygulama ekibi ve gerekirse platform/DevOps escalation sahibi yazılmalı |
| DLQ kayıtlarını kim triage eder? | Uygulama owner, schema/platform owner desteğiyle |
| Replay'i kim onaylar? | Service owner veya incident commander |
| İlk yanıt SLA'i nedir? | Warning ve critical için ayrı SLA tanımlanmalı |
| Dashboard ne göstermeli? | Retry count, retry exhausted, DLQ count, DLQ publish failure, source topic lag ve consumer lag |

`DLQ Messages Detected` alert'i otomatik replay başlatmaz; triage ve incident kaydı başlatır.

---

## Alert Yanıtlama

### Alert: Retry Spike

Retry artışı her zaman incident değildir; transient bir bağımlılık kısa süreli toparlanıyor olabilir. Yine de aşağıdakiler kontrol edilmelidir:

1. Retry hangi source topic ve exception tipinde yoğunlaşıyor?
2. Retry sonrası kayıtlar başarıyla işleniyor mu, yoksa `retry.exhausted` artıyor mu?
3. Consumer lag artıyor mu?
4. Non-retryable olması gereken bir hata yanlışlıkla retry ediliyor mu?
5. Backoff toplamı partition'ı gereksiz bekletiyorsa retry topic pattern'i ihtiyacı aç.

### Alert: Retry Exhausted

Bu alert maksimum retry sonrası kayıtların DLQ'ye taşındığını gösterir.

1. DLQ mesajlarını exception tipine göre grupla.
2. Aynı exception için yeni deploy/config/schema değişikliği var mı kontrol et.
3. Hata transient ise retry sayısı/backoff yeterli mi değerlendir.
4. Hata kalıcı ise retry politikasına non-retryable fast-path ekle.

### Alert: DLQ Messages Detected

1. Metric boyutlarını kontrol et: `topic`, `source_topic`, `exception`.
2. Aynı zaman aralığındaki application loglarını ve trace'leri incele.
3. Kaydın transient mi kalıcı mı başarısız olduğunu sınıflandır.
4. Root cause giderilmeden replay başlatma.
5. Incident kaydına DLQ topic, partition, offset ve exception bilgisini ekle.

### Alert: DLQ Publish Failure

Bu alert kritik kabul edilmelidir; çünkü başarısız kayıt DLQ'ye de yazılamıyor olabilir.

Kontroller:

- DLQ topic **CDLZ cluster**'ında (`FDP_APP_CDL_KAFKA_BROKER`) gerçekten var mı?
- DLQ KafkaTemplate CDLZ cluster'ını ve schema registry'sini hedefliyor mu?
- Command adaptor'ın DLQ topic'e produce ACL'i var mı?
- DLQ KafkaTemplate serializer ayarları payload tipini destekliyor mu?
- DLQ topic partition sayısı source partition ile uyumlu mu (ya da recoverer partition `-1` mi kullanıyor)?

---

## DLQ İnceleme Komutları

Topic listesini kontrol et:

```bash
kafka-topics.sh --list \
  --bootstrap-server kafka:29092
```

DLQ topic detayını kontrol et:

```bash
kafka-topics.sh --describe \
  --topic landing-1-dlq \
  --bootstrap-server kafka:29092
```

DLQ mesajlarını incele:

```bash
kafka-console-consumer.sh \
  --topic landing-1-dlq \
  --from-beginning \
  --max-messages 10 \
  --bootstrap-server kafka:29092
```

Production ortamında bu komutlar doğrudan çalıştırılmadan önce erişim, veri hassasiyeti ve audit gereksinimleri kontrol edilmelidir.

---

## Incident Kayıt Şablonu

Her DLQ/retry olayı için aşağıdaki alanlar doldurulmalıdır:

| Alan | Değer |
|------|-------|
| Date/time | |
| Environment | |
| Source topic | |
| DLQ topic | |
| Partition/offset range | |
| Exception class | |
| Sample correlation/reference id | |
| Same-window deployment/config changes | |
| Schema version | |
| Triage owner | |
| Replay decision | |
| Replay approver | |
| Outcome | |

---

## Reprocessing Prosedürü

Reprocessing yalnızca root cause düzeltildikten sonra yapılmalıdır.

DLQ mesajı olması replay yapılacağı anlamına gelmez. Özellikle EORI için replay kararı, duplicate output etkisi anlaşılmadan verilmemelidir.

### Reprocessor Yoksa

1. DLQ kayıtlarını silme.
2. İlgili partition/offset listesini incident kaydına ekle.
3. Replay için onaylı script veya platform prosedürü kullan.
4. Replay öncesi dry-run ve kayıt sayısı kontrolü yap.
5. Replay sonrası DLQ count ve normal output topic'lerini doğrula.

### Reprocessor Geliştirildiyse

Reprocessor aşağıdaki kontroller olmadan kullanılmamalıdır:

- Dry-run mode
- Tek kayıt/offset aralığı seçimi
- Rate limit
- Audit log
- Yetki kontrolü
- Duplicate handling stratejisi

---

## Sorun Giderme

### DLQ'ye Mesaj Gitmiyor

Kontrol listesi:

- [ ] Listener içinde exception yutuluyor mu?
- [ ] `KafkaTemplate.send()` future'ı bekleniyor mu?
- [ ] `DefaultErrorHandler` listener container factory'ye bağlandı mı?
- [ ] `app.dlq.enabled=true` mi? (`false` iken `DlqConfig` bean'i oluşturulmaz.)
- [ ] Deserialization/schema hataları için consumer `ErrorHandlingDeserializer` kullanıyor mu? Kullanmıyorsa bu hatalar poll döngüsünü kırar ve error handler'a ya da DLQ'ye hiç ulaşmaz.
- [ ] DLQ topic, source topic'lerin tüketildiği **CDLZ cluster**'ında mı mevcut (adaptör cluster'ında değil)?
- [ ] DLQ `KafkaTemplate` CDLZ cluster'ına ve schema registry'sine yönlendirilmiş mi?
- [ ] DLQ topic source kadar (ya da daha fazla) partition'a sahip mi (ya da recoverer partition `-1` mi kullanıyor)?
- [ ] DLQ producer serializer doğru mu?
- [ ] ACL produce izni var mı?

### Retry Çok Fazla Çalışıyor

Kontrol listesi:

- [ ] Hata tipi gerçekten retryable mı?
- [ ] Aynı payload sürekli aynı exception'ı mı üretiyor?
- [ ] `max-retries`, `interval-ms`, `multiplier` ve `max-interval-ms` beklenen değerlerde mi?
- [ ] Consumer lag artıyor mu?
- [ ] Uzun bekleme gerekiyorsa retry topic pattern'i daha doğru mu?

### Çok Fazla DLQ Mesajı Var

Kontrol listesi:

- [ ] Hata tipi tek bir exception'a mı yoğunlaşıyor?
- [ ] Son deploy sonrası mı başladı?
- [ ] Schema değişikliği var mı?
- [ ] Source topic'te beklenmeyen payload geldi mi?
- [ ] Downstream Kafka/Schema Registry erişilebilir mi?

### Reprocessing Başarısız

1. Replay'i durdur.
2. Yeni DLQ kayıtlarının exception tipini karşılaştır.
3. Kalıcı data/schema hatası varsa migration veya code fix olmadan tekrar deneme.
4. Duplicate output riski varsa tüketici/idempotency etkisini değerlendir.

---

## Geri Alma

1. `app.dlq.enabled=false` ile DLQ davranışını kapat.
2. Retry konfigürasyonu `DlqConfig` dışında ayrıca bağlıysa devre dışı bırakılmalı veya geri alınmalıdır; örneğin desteklenen yerlerde `max-retries=0` ayarı yapılabilir.
3. Yeni listener exception propagation davranışı rollback planında açıkça değerlendirilmeli; eski pattern'e dönmek veri kaybı riskini geri getirir.
4. DLQ topic'leri ve mesajları silinmemelidir.

---

## İlgili Belgeler

- [Kafka Retry ve DLQ Discovery Önerisi](00-dlq-neden-yapilmali.md)
- [Teknik Analiz](01-teknik-analiz.md)
- [Uygulama Kılavuzu](02-uygulama-kilavuzu.md)
