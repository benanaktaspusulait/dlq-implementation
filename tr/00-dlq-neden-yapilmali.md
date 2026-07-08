# Dead Letter Queue (DLQ) Önerisi: Neden Yapılmalı?

| Alan | Değer |
|------|-------|
| Sahip | Benan Aktas |
| Durum | İncelemede |
| Oluşturulma | 2026-07-08 |
| Son güncelleme | 2026-07-09 |
| Kapsam | `/Users/benanaktas/project/home-office/test1` içindeki `cmd-adaptor-sns` projesi |
| Etiketler | kafka, dlq, error-handling, data-integrity |

---

## Ana Mesaj

1. `cmd-adaptor-sns` içinde Kafka listener hataları uygulama kodunda yakalanıp sadece loglanıyor.
2. Bu davranış Spring Kafka retry/error handler zincirini devre dışı bırakıyor; başarısız kayıtlar geri kazanılabilir bir yere yazılmıyor.
3. DLQ, retry, metric ve runbook birlikte ele alınırsa veri kaybı riski azalır ve operasyonel görünürlük oluşur.

---

## İncelenen Kanıtlar

| Kaynak | Bulgular |
|--------|----------|
| `cmd-adaptor-sns/src/main/java/uk/gov/ho/dacc/fdp/listener/KafkaSourceListener.java` | `@KafkaListener` içinde `try/catch` var; exception sadece `log.error` ile kapanıyor. |
| `cmd-adaptor-sns/src/main/java/uk/gov/ho/dacc/fdp/listener/KafkaLookupEoriListener.java` | Aynı pattern EORI lookup listener'da da var. Ayrıca bir landing kaydı birden fazla async producer send'i üretiyor. |
| `cmd-adaptor-sns/src/main/resources/application.yml` | Giriş topic'leri `landing-1` ve `landing-413`; DLQ topic/property tanımı yok. |
| `cmd-adaptor-sns/src/main/resources/application.yml` | Dynatrace/Prometheus altyapısı var, fakat DLQ/retry metric'i yok. |
| `cmd-adaptor-sns-integration-tests/pom.xml` | Entegrasyon testi docker-compose profilleriyle çalışıyor; Testcontainers bağımlılığı görünmüyor. |

Bu belge SNS adaptörü için doğrulanmış bulgulara dayanır. Diğer FDP adaptörleri aynı pattern'i kullanıyor olabilir, fakat bu dokümanda ayrıca doğrulanmadıkları için rollout kapsamına "validasyon sonrası" alınmalıdır.

---

## Mevcut Durum

`cmd-adaptor-sns` iki ana Kafka girişini tüketiyor: CDLZ SNS landing akışı ve EORI landing akışı. Her iki listener da işleme sırasında oluşan hatayı yakalayıp logluyor. Exception listener metodundan dışarı çıkmadığı için Spring Kafka'nın retry ve `DefaultErrorHandler` davranışı devreye giremiyor. Ayrıca `KafkaTemplate.send()` async çalıştığı için producer ack hataları beklenmezse listener başarılı tamamlanmış gibi davranabilir.

---

## Üst Gözlemler

| # | Gözlem | Etki | Öneri | Güven |
|---|--------|------|-------|-------|
| 1 | Listener seviyesinde `try/catch + log.error` var | Başarısız kayıt commit edilebilir ve kaybolabilir | Hataları yutma; Spring Kafka error handler'a bırak | Yüksek |
| 2 | DLQ topic/property yok | Başarısız kayıtlar geri kazanılamaz | Tüketilen giriş topic'leri için DLQ tanımla | Yüksek |
| 3 | Producer send sonucu beklenmiyor | Async send hataları retry/DLQ akışına girmeyebilir | Send future'larını bekle veya hatayı listener thread'ine taşı | Yüksek |
| 4 | DLQ metric/alert yok | Hata ancak log aramasıyla bulunur | DLQ ve retry metric'leri ekle | Yüksek |
| 5 | Reprocessing akışı yok | Recovery manuel ve riskli kalır | İlk fazda manuel prosedür, sonraki fazda kontrollü reprocessor | Orta |

---

## Neden DLQ?

### Veri Kaybını Önleme

Bugünkü akış:

```text
Kafka kaydı -> Listener -> Hata -> log.error -> kayıt başarıyla tüketilmiş gibi kapanabilir
```

Hedef akış:

```text
Kafka kaydı -> Listener -> Hata dışarı fırlatılır -> retry -> hala başarısızsa DLQ -> metric/alert
```

DLQ, hatalı kaydı orijinal payload ve hata metadata'sı ile korur. Bu sayede kayıt yeniden üretilebilen, incelenebilen ve kontrollü şekilde tekrar işlenebilen bir artefakt olur.

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

**Birincil öneri:** Spring Kafka `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` + zenginleştirilmiş DLQ metadata.

Bu seçenek custom bir hata altyapısı yazmadan Spring Kafka'nın listener retry ve recoverer davranışını kullanır. Proje özelinde kritik nokta, mevcut listener `try/catch` bloklarının hatayı yutmaması ve producer send hatalarının listener thread'ine taşınmasıdır.

### DLQ Topic Kuralı

DLQ topic'i output topic'e değil, listener'ın tükettiği giriş topic'ine bağlanmalıdır:

| Listener | Tüketilen topic property | Local default | Önerilen DLQ |
|----------|--------------------------|---------------|--------------|
| `KafkaSourceListener` | `app.topic.cdlz-incoming` | `landing-1` | `${app.topic.cdlz-incoming}-dlq` |
| `KafkaLookupEoriListener` | `app.lookup.eori.cdlz-incoming` | `landing-413` | `${app.lookup.eori.cdlz-incoming}-dlq` |

Eğer platform topic isimlerinde suffix kullanımına izin vermiyorsa `app.dlq.topics.source` ve `app.dlq.topics.eori` explicit property olarak verilmelidir.

---

## Çözüm Seçenekleri

| Seçenek | Açıklama | Artı | Eksi |
|---------|----------|------|------|
| A | Sadece `DefaultErrorHandler` + DLQ | En hızlı pilot, az kod | Metadata/metric sınırlı kalabilir |
| B | `DefaultErrorHandler` + explicit DLQ resolver + metadata + metric | Önerilen denge; audit ve alert destekler | Biraz daha fazla konfigürasyon/test gerekir |
| C | Retry topic pattern + DLQ | Uzun backoff ve yüksek hacim için güçlü | Daha fazla topic, daha fazla operasyonel karmaşıklık |

---

## Fazlı Plan

### Faz 1: SNS Pilot

- [ ] `cmd-adaptor-sns` içinde DLQ configuration property'lerini ekle.
- [ ] `KafkaSourceListener` ve `KafkaLookupEoriListener` içindeki hata yutan `try/catch` pattern'ini kaldır veya rethrow edecek şekilde değiştir.
- [ ] Producer send future'larını bekleyerek async send hatalarını görünür yap.
- [ ] `DefaultErrorHandler` ve `DeadLetterPublishingRecoverer` ile retry + DLQ davranışını konfigüre et.
- [ ] DLQ payload serializer'ının `GenericRecord` ve `CdlzLandingRecord` için çalıştığını doğrula.
- [ ] Unit testlerde listener'ın exception'ı yutmadığını ve error handler bean'inin yüklendiğini doğrula.
- [ ] Mevcut docker-compose entegrasyon profiline DLQ senaryosu ekle.

### Faz 2: Monitoring ve Runbook

- [ ] `fdp.dlq.messages.total` ve `fdp.kafka.listener.retry.total` benzeri metric'leri ekle.
- [ ] Dynatrace/Prometheus dashboard ve alert threshold'larını tanımla.
- [ ] Runbook'u gerçek topic isimleri, alert adları ve manuel replay prosedürüyle güncelle.

### Faz 3: Kontrollü Reprocessing

- [ ] Manuel replay prosedürü yeterli değilse ayrı bir DLQ reprocessor tasarla.
- [ ] Reprocessing için audit log, yetki kontrolü, dry-run ve rate limit ekle.
- [ ] EORI listener'da çoklu output send nedeniyle partial success/duplicate riskini ayrıca ele al.

---

## Ana Riskler

| Risk | Etki | Azaltma |
|------|------|---------|
| Listener exception'ı yutmaya devam eder | Error handler çalışmaz | Try/catch kaldırma/rethrow için test yaz |
| Async send hatası listener'a dönmez | Producer hatası DLQ'ye düşmez | Send future'ını bekle veya callback hatasını kontrollü taşı |
| DLQ serializer yanlış olur | DLQ publish de başarısız olur | DLQ için Avro serializer entegrasyon testi ekle |
| EORI partial output oluşur | Retry duplicate kayıt üretebilir | Idempotent key, transactional send veya duplicate handling stratejisi belirle |
| DLQ topic yanlış isimlenir | Operasyon topic'i bulamaz | Tüketilen topic bazlı explicit mapping kullan |

---

## İlgili Belgeler

- [Teknik Analiz](01-teknik-analiz.md)
- [Uygulama Kılavuzu](02-uygulama-kilavuzu.md)
- [Runbook](03-runbook.md)
