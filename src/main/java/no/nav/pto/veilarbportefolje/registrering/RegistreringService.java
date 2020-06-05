package no.nav.pto.veilarbportefolje.registrering;

import no.nav.arbeid.soker.registrering.ArbeidssokerRegistrertEvent;
import no.nav.pto.veilarbportefolje.domene.AktoerId;
import no.nav.pto.veilarbportefolje.elastic.ElasticIndexer;
import no.nav.pto.veilarbportefolje.elastic.domene.OppfolgingsBruker;
import no.nav.pto.veilarbportefolje.kafka.KafkaConsumerService;
import no.nav.pto.veilarbportefolje.service.AktoerService;
import no.nav.pto.veilarbportefolje.util.Result;

import java.time.ZonedDateTime;
import java.util.Optional;

public class RegistreringService implements KafkaConsumerService<ArbeidssokerRegistrertEvent> {
    private final RegistreringRepository registreringRepository;
    private final ElasticIndexer elasticIndexer;
    private final AktoerService aktoerService;

    public RegistreringService(RegistreringRepository registreringRepository, ElasticIndexer elasticIndexer, AktoerService aktoerService) {
        this.registreringRepository = registreringRepository;
        this.elasticIndexer = elasticIndexer;
        this.aktoerService = aktoerService;
    }

    public void behandleKafkaMelding(ArbeidssokerRegistrertEvent kafkaRegistreringMelding) {
        AktoerId aktoerId = AktoerId.of(kafkaRegistreringMelding.getAktorid());
        registreringRepository.upsertBrukerRegistrering(kafkaRegistreringMelding);
        Result<OppfolgingsBruker> oppfolgingsBrukerResult = elasticIndexer.indekser(aktoerId);

        if(oppfolgingsBrukerResult.isErr()) {
            aktoerService.hentFnrFraAktorId(aktoerId)
                    .onSuccess(elasticIndexer::indekser);
        }
    }

    public Optional<ZonedDateTime> hentRegistreringOpprettet(AktoerId aktoerId) {
        return registreringRepository
                .hentBrukerRegistrering(aktoerId)
                .map(ArbeidssokerRegistrertEvent::getRegistreringOpprettet)
                .map(ZonedDateTime::parse);
    }
}
