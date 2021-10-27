package no.nav.pto.veilarbportefolje.oppfolging;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.nav.common.json.JsonUtils;
import no.nav.common.types.identer.AktorId;
import no.nav.pto.veilarbportefolje.arbeidsliste.ArbeidslisteRepositoryV1;
import no.nav.pto.veilarbportefolje.arbeidsliste.ArbeidslisteService;
import no.nav.pto.veilarbportefolje.cv.CVRepositoryV2;
import no.nav.pto.veilarbportefolje.cv.CvRepository;
import no.nav.pto.veilarbportefolje.elastic.ElasticServiceV2;
import no.nav.pto.veilarbportefolje.kafka.KafkaCommonConsumerService;
import no.nav.pto.veilarbportefolje.kafka.KafkaConsumerService;
import no.nav.pto.veilarbportefolje.registrering.RegistreringService;
import no.nav.pto.veilarbportefolje.service.UnleashService;
import no.nav.pto.veilarbportefolje.sisteendring.SisteEndringService;
import org.springframework.stereotype.Service;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import static java.time.Instant.EPOCH;
import static java.time.ZonedDateTime.ofInstant;


@Slf4j
@Service
@RequiredArgsConstructor
public class OppfolgingAvsluttetService extends KafkaCommonConsumerService<OppfolgingAvsluttetDTO> implements KafkaConsumerService<String> {
    private final ArbeidslisteService arbeidslisteService;
    private final ArbeidslisteRepositoryV1 arbeidslisteRepositoryV2;
    private final OppfolgingRepository oppfolgingRepository;
    private final OppfolgingRepositoryV2 oppfolgingRepositoryV2;
    private final RegistreringService registreringService;
    private final CvRepository cvRepository;
    private final CVRepositoryV2 cvRepositoryV2;
    private final ElasticServiceV2 elasticServiceV2;
    private final SisteEndringService sisteEndringService;
    @Getter
    private final UnleashService unleashService;

    @Override
    public void behandleKafkaMelding(String kafkaMelding) {
        final OppfolgingAvsluttetDTO dto = JsonUtils.fromJson(kafkaMelding, OppfolgingAvsluttetDTO.class);
        behandleKafkaMeldingLogikk(dto);
    }

    @Override
    public void behandleKafkaMeldingLogikk(OppfolgingAvsluttetDTO dto) {
        final AktorId aktoerId = dto.getAktorId();


        final ZonedDateTime startDato = oppfolgingRepository.hentStartdato(aktoerId).orElse(ofInstant(EPOCH, ZoneId.systemDefault()));

        final ZonedDateTime sluttDato = dto.getSluttdato();
        if (startDato.isAfter(sluttDato)) {
            log.warn("Lagret startdato for oppfølging er etter mottatt sluttdato for bruker {}", aktoerId);
            return;
        }

        avsluttOppfolging(aktoerId);
    }

    public void avsluttOppfolging(AktorId aktoerId) {
        oppfolgingRepository.slettOppfolgingData(aktoerId);
        oppfolgingRepositoryV2.slettOppfolgingData(aktoerId);
        registreringService.slettRegistering(aktoerId);
        arbeidslisteService.slettArbeidsliste(aktoerId);
        arbeidslisteRepositoryV2.slettArbeidsliste(aktoerId);// TODO: slett denne linjen når vi kun bruker postgres
        sisteEndringService.slettSisteEndringer(aktoerId);
        cvRepository.resetHarDeltCV(aktoerId);
        cvRepositoryV2.resetHarDeltCV(aktoerId);

        elasticServiceV2.slettDokumenter(List.of(aktoerId));
        log.info("Bruker: {} har avsluttet oppfølging og er slettet", aktoerId);
    }

    @Override
    public boolean shouldRewind() {
        return false;
    }

    @Override
    public void setRewind(boolean rewind) {

    }
}
