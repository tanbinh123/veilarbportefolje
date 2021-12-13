package no.nav.pto.veilarbportefolje.cv;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.nav.arbeid.cv.avro.Melding;
import no.nav.arbeid.cv.avro.Meldingstype;
import no.nav.common.types.identer.AktorId;
import no.nav.pto.veilarbportefolje.cv.dto.CVMelding;
import no.nav.pto.veilarbportefolje.elastic.ElasticServiceV2;
import no.nav.pto.veilarbportefolje.kafka.KafkaCommonConsumerService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.util.List;

import static no.nav.pto.veilarbportefolje.cv.dto.Ressurs.CV_HJEMMEL;


@RequiredArgsConstructor
@Service
@Slf4j
public class CVService extends KafkaCommonConsumerService<Melding> {
    private final ElasticServiceV2 elasticServiceV2;
    private final CvRepository cvRepository;
    private final CVRepositoryV2 cvRepositoryV2;

    @Override
    public void behandleKafkaMeldingLogikk(Melding kafkaMelding) {
        AktorId aktoerId = AktorId.of(kafkaMelding.getAktoerId());

        boolean cvEksisterer = cvEksistere(kafkaMelding);
        cvRepositoryV2.upsertCVEksisterer(aktoerId, cvEksisterer);
        cvRepository.upsertCvEksistere(aktoerId, cvEksisterer);
        elasticServiceV2.updateCvEksistere(aktoerId, cvEksisterer);
    }

    public void behandleKafkaMeldingCVHjemmel(ConsumerRecord<String, CVMelding> kafkaMelding) {
        log.info(
                "Behandler kafka-melding med key {} og offset {} på topic {}",
                kafkaMelding.key(),
                kafkaMelding.offset(),
                kafkaMelding.topic()
        );
        CVMelding cvMelding = kafkaMelding.value();
        behandleCVHjemmelMelding(cvMelding);
    }

    public void behandleCVHjemmelMelding(CVMelding cvMelding) {
        AktorId aktoerId = cvMelding.getAktoerId();
        boolean harDeltCv = (cvMelding.getSlettetDato() == null);

        if (cvMelding.getRessurs() != CV_HJEMMEL) {
            log.info("Ignorer melding for ressurs {} for bruker {}", cvMelding.getRessurs(), aktoerId);
            return;
        }

        log.info("Oppdaterte bruker: {}. Har delt cv: {}", aktoerId, harDeltCv);
        cvRepositoryV2.upsertHarDeltCv(aktoerId, harDeltCv);
        cvRepository.upsertHarDeltCv(aktoerId, harDeltCv);

        elasticServiceV2.updateHarDeltCv(aktoerId, harDeltCv);
    }

    public void migrerTilPostgres() {
        List<AktorId> alleBrukereMedCvData = cvRepository.hentAlleBrukereMedCvData();
        log.info("Migrering av CV for {} brukere.", alleBrukereMedCvData.size());
        alleBrukereMedCvData.forEach(bruker -> {
                    try {
                        Boolean deltCv = cvRepository.harDeltCv(bruker);
                        Boolean cvEksisterer = cvRepository.harCvEksisterer(bruker);

                        cvRepositoryV2.upsertCVEksisterer(bruker, cvEksisterer);
                        cvRepositoryV2.upsertHarDeltCv(bruker, deltCv);
                    } catch (Exception e) {
                        log.error("Migrering feilet på bruker: {}", bruker, e);
                    }
                }
        );

        int brukereMedCvData = cvRepository.hentAlleBrukereMedCvData().size();
        int brukereMedCvDataPostgres = cvRepositoryV2.hentAlleBrukereMedCvData().size();
        log.info("Migrering av CV data er ferdig. Brukere i oracle: {}, i Postgres: {}", brukereMedCvData, brukereMedCvDataPostgres);
    }

    private boolean cvEksistere(Melding melding) {
        return melding.getMeldingstype() == Meldingstype.ENDRE || melding.getMeldingstype() == Meldingstype.OPPRETT;
    }
}
