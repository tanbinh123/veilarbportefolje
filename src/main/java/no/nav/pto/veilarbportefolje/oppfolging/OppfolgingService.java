package no.nav.pto.veilarbportefolje.oppfolging;

import lombok.extern.slf4j.Slf4j;
import no.nav.pto.veilarbportefolje.arbeidsliste.ArbeidslisteService;
import no.nav.pto.veilarbportefolje.domene.AktoerId;
import no.nav.pto.veilarbportefolje.domene.VeilederId;
import no.nav.pto.veilarbportefolje.elastic.ElasticIndexer;
import no.nav.pto.veilarbportefolje.kafka.KafkaConsumerService;
import no.nav.pto.veilarbportefolje.service.NavKontorService;
import no.nav.pto.veilarbportefolje.service.VeilederService;
import no.nav.pto.veilarbportefolje.util.Result;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

import static no.nav.json.JsonUtils.fromJson;


@Slf4j
public class OppfolgingService implements KafkaConsumerService {

    private final OppfolgingRepository oppfolgingRepository;
    private final ElasticIndexer elastic;
    private final VeilederService veilederService;
    private final NavKontorService navKontorService;
    private final ArbeidslisteService arbeidslisteService;

    public OppfolgingService(OppfolgingRepository oppfolgingRepository, ElasticIndexer elastic, VeilederService veilederService, NavKontorService navKontorService, ArbeidslisteService arbeidslisteService) {
        this.oppfolgingRepository = oppfolgingRepository;
        this.elastic = elastic;
        this.veilederService = veilederService;
        this.navKontorService = navKontorService;
        this.arbeidslisteService = arbeidslisteService;
    }

    @Override
    @Transactional
    public void behandleKafkaMelding(String kafkaMelding) {
        OppfolgingStatus oppfolgingStatus = fromJson(kafkaMelding, OppfolgingStatus.class);
        AktoerId aktoerId = oppfolgingStatus.getAktoerId();

        if (oppfolgingStatus.getStartDato() == null) {
            log.warn("Bruker {} har ikke startDato", aktoerId);
        }

        if (brukerenIkkeLengerErUnderOppfolging(oppfolgingStatus) || eksisterendeVeilederHarIkkeTilgangTilBruker(aktoerId)) {
            Result<Integer> result = arbeidslisteService.deleteArbeidslisteForAktoerId(aktoerId);
            if (result.isErr()) {
                log.error("Kunne ikke slette arbeidsliste for bruker {}", aktoerId);
            }
        }

        oppfolgingRepository.oppdaterOppfolgingData(oppfolgingStatus)
                .orElseThrowException();

        elastic.indekser(aktoerId)
                .orElseThrowException();
    }

    boolean eksisterendeVeilederHarIkkeTilgangTilBruker(AktoerId aktoerId) {
        return !eksisterendeVeilederHarTilgangTilBruker(aktoerId);
    }

    boolean eksisterendeVeilederHarTilgangTilBruker(AktoerId aktoerId) {
        Optional<VeilederId> eksisterendeVeileder = oppfolgingRepository.hentOppfolgingData(aktoerId).ok()
                .map(info -> info.getVeileder())
                .map(VeilederId::new);

        if (!eksisterendeVeileder.isPresent()) {
            return false;
        }

        Result<List<VeilederId>> result = navKontorService.hentEnhetForBruker(aktoerId)
                .map(enhet -> veilederService.hentVeilederePaaEnhet(enhet));

        return eksisterendeVeileder
                .flatMap(veileder -> result.ok().map(veilederePaaEnhet -> veilederePaaEnhet.contains(veileder)))
                .orElse(false);
    }

    static boolean brukerenIkkeLengerErUnderOppfolging(OppfolgingStatus oppfolgingStatus) {
        return !oppfolgingStatus.isOppfolging();
    }
}