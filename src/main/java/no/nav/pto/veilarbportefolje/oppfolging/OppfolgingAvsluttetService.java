package no.nav.pto.veilarbportefolje.oppfolging;

import lombok.extern.slf4j.Slf4j;
import no.nav.pto.veilarbportefolje.arbeidsliste.ArbeidslisteService;
import no.nav.pto.veilarbportefolje.domene.AktoerId;
import no.nav.pto.veilarbportefolje.domene.Fnr;
import no.nav.pto.veilarbportefolje.elastic.ElasticServiceV2;
import no.nav.pto.veilarbportefolje.kafka.KafkaConsumerService;
import no.nav.pto.veilarbportefolje.service.BrukerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static no.nav.common.json.JsonUtils.fromJson;

@Slf4j
@Service
public class OppfolgingAvsluttetService implements KafkaConsumerService<String> {

    private final ArbeidslisteService arbeidslisteService;
    private final OppfolgingRepository oppfolgingRepository;
    private final ElasticServiceV2 elasticServiceV2;
    private final BrukerService brukerService;

    @Autowired
    public OppfolgingAvsluttetService(ArbeidslisteService arbeidslisteService, OppfolgingRepository oppfolgingStatusRepository, ElasticServiceV2 elasticServiceV2, BrukerService brukerService) {
        this.arbeidslisteService = arbeidslisteService;
        this.oppfolgingRepository = oppfolgingStatusRepository;
        this.elasticServiceV2 = elasticServiceV2;
        this.brukerService = brukerService;
    }

    @Override
    public void behandleKafkaMelding(String kafkaMelding) {
        OppfolgingAvsluttetDTO dto = fromJson(kafkaMelding, OppfolgingAvsluttetDTO.class);
        AktoerId aktorId = dto.getAktorId();

        log.info("Bruker {} er ikke lenger under oppfølging", aktorId.toString());
        oppfolgingRepository.avsluttOppfolging(aktorId);

        Fnr fnr = brukerService.hentFnr(aktorId).orElseThrow();
        arbeidslisteService.slettArbeidsliste(fnr);
        elasticServiceV2.avsluttOppfolging(fnr);
    }

    @Override
    public boolean shouldRewind() {
        return false;
    }

    @Override
    public void setRewind(boolean rewind) {

    }
}
