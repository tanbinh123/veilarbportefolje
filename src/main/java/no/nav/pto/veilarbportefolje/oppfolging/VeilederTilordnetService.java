package no.nav.pto.veilarbportefolje.oppfolging;

import no.nav.common.featuretoggle.UnleashService;
import no.nav.common.json.JsonUtils;
import no.nav.common.types.identer.EnhetId;
import no.nav.pto.veilarbportefolje.arbeidsliste.ArbeidslisteService;
import no.nav.common.types.identer.AktorId;
import no.nav.pto.veilarbportefolje.client.VeilarbVeilederClient;
import no.nav.pto.veilarbportefolje.database.BrukerRepositoryV2;
import no.nav.pto.veilarbportefolje.domene.value.VeilederId;
import no.nav.pto.veilarbportefolje.elastic.ElasticServiceV2;
import no.nav.pto.veilarbportefolje.kafka.KafkaConsumerService;
import no.nav.pto.veilarbportefolje.oppfolgingsbruker.OppfolginsbrukerRepositoryV2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;

import static no.nav.pto.veilarbportefolje.config.FeatureToggle.erPostgresPa;

@Service
public class VeilederTilordnetService implements KafkaConsumerService<String> {

    private final OppfolgingRepository oppfolgingRepository;
    private final OppfolgingRepositoryV2 oppfolgingRepositoryV2;
    private final BrukerRepositoryV2 brukerRepositoryV2;
    private final ArbeidslisteService arbeidslisteService;
    private final ArbeidslisteService arbeidslisteServicePostgres;
    private final UnleashService unleashService;
    private final VeilarbVeilederClient veilarbVeilederClient;
    private final ElasticServiceV2 elasticServiceV2;

    @Autowired
    public VeilederTilordnetService(@Qualifier("PostgresArbeidslisteService") ArbeidslisteService arbeidslisteServicePostgres, OppfolgingRepository oppfolgingRepository, OppfolgingRepositoryV2 oppfolgingRepositoryV2, OppfolginsbrukerRepositoryV2 oppfolginsbrukerRepositoryV2, BrukerRepositoryV2 brukerRepositoryV2, ArbeidslisteService arbeidslisteService, ElasticServiceV2 elasticServiceV2, UnleashService unleashService, VeilarbVeilederClient veilarbVeilederClient) {
        this.oppfolgingRepository = oppfolgingRepository;
        this.oppfolgingRepositoryV2 = oppfolgingRepositoryV2;
        this.brukerRepositoryV2 = brukerRepositoryV2;
        this.arbeidslisteService = arbeidslisteService;
        this.arbeidslisteServicePostgres = arbeidslisteServicePostgres;
        this.elasticServiceV2 = elasticServiceV2;
        this.unleashService = unleashService;
        this.veilarbVeilederClient = veilarbVeilederClient;
    }

    @Override
    public void behandleKafkaMelding(String kafkaMelding) {
        final VeilederTilordnetDTO dto = JsonUtils.fromJson(kafkaMelding, VeilederTilordnetDTO.class);
        final AktorId aktoerId = dto.getAktorId();
        oppfolgingRepository.settVeileder(aktoerId, dto.getVeilederId());

        if (erPostgresPa(unleashService)) {
            oppfolgingRepositoryV2.settVeileder(aktoerId, dto.getVeilederId());
            brukerRepositoryV2.getNavKontor(aktoerId)
                    .ifPresent(s -> settUfordeltStatus(aktoerId, EnhetId.of(s), dto.getVeilederId()));
        }

        elasticServiceV2.oppdaterVeileder(aktoerId, dto.getVeilederId());

        // TODO: Slett oracle basert kode naar vi er over paa postgres.
        final boolean harByttetNavKontorPostgres = arbeidslisteServicePostgres.brukerHarByttetNavKontor(aktoerId);
        if (harByttetNavKontorPostgres) {
            arbeidslisteServicePostgres.slettArbeidsliste(aktoerId);
        }

        final boolean harByttetNavKontor = arbeidslisteService.brukerHarByttetNavKontor(aktoerId);
        if (harByttetNavKontor) {
            arbeidslisteService.slettArbeidsliste(aktoerId);
        }
    }

    private void settUfordeltStatus(AktorId aktorId, EnhetId enhetId, VeilederId veilederId){
        List<String> veilederePaaEnhet = veilarbVeilederClient.hentVeilederePaaEnhet(enhetId);
        oppfolgingRepositoryV2.settUfordeltStatus(aktorId.get(), veilederePaaEnhet.contains(veilederId.getValue()));
    }

    @Override
    public boolean shouldRewind() {
        return false;
    }

    @Override
    public void setRewind(boolean rewind) {
    }
}
