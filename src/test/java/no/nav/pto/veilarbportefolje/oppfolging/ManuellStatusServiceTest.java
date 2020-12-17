package no.nav.pto.veilarbportefolje.oppfolging;

import no.nav.pto.veilarbportefolje.domene.BrukerOppdatertInformasjon;
import no.nav.pto.veilarbportefolje.domene.value.AktoerId;
import no.nav.pto.veilarbportefolje.util.ElasticTestClient;
import no.nav.pto.veilarbportefolje.util.EndToEndTest;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static no.nav.pto.veilarbportefolje.domene.ManuellBrukerStatus.MANUELL;
import static no.nav.pto.veilarbportefolje.util.ElasticTestClient.pollElasticUntil;
import static no.nav.pto.veilarbportefolje.util.TestDataUtils.randomAktoerId;
import static org.assertj.core.api.Assertions.assertThat;


class ManuellStatusServiceTest extends EndToEndTest {

    private final OppfolgingRepository oppfolgingRepository;
    private final ManuellStatusService manuellStatusService;
    private final ElasticTestClient elasticTestClient;

    @Autowired
    public ManuellStatusServiceTest(OppfolgingRepository oppfolgingRepository, ManuellStatusService manuellStatusService, ElasticTestClient elasticTestClient) {
        this.oppfolgingRepository = oppfolgingRepository;
        this.manuellStatusService = manuellStatusService;
        this.elasticTestClient = elasticTestClient;
    }

    @Test
    void skal_oppdatere_oversikten_når_bruker_blir_satt_til_manuell() {
        final AktoerId aktoerId = randomAktoerId();
        final BrukerOppdatertInformasjon info = new BrukerOppdatertInformasjon()
                .setAktoerid(aktoerId.toString())
                .setManuell(false);

        oppfolgingRepository.oppdaterOppfolgingData(info);
        elasticTestClient.createUserInElastic(aktoerId);

        String melding = new JSONObject()
                .put("aktorId", aktoerId.toString())
                .put("erManuell", true)
                .toString();

        manuellStatusService.behandleKafkaMelding(melding);

        final BrukerOppdatertInformasjon oppfolgingData = oppfolgingRepository.hentOppfolgingData(aktoerId).orElseThrow();

        assertThat(oppfolgingData.getManuell()).isTrue();
        pollElasticUntil(() -> elasticTestClient.hentBrukerFraElastic(aktoerId).getManuell_bruker().equals(MANUELL.name()));
    }

    @Test
    void skal_oppdatere_oversikten_når_bruker_blir_satt_til_digital_oppfølging() {
        final AktoerId aktoerId = randomAktoerId();
        final BrukerOppdatertInformasjon info = new BrukerOppdatertInformasjon()
                .setAktoerid(aktoerId.toString())
                .setManuell(false);

        oppfolgingRepository.oppdaterOppfolgingData(info);
        elasticTestClient.createUserInElastic(aktoerId);

        String melding = new JSONObject()
                .put("aktorId", aktoerId.toString())
                .put("erManuell", false)
                .toString();

        manuellStatusService.behandleKafkaMelding(melding);

        final BrukerOppdatertInformasjon oppfolgingData = oppfolgingRepository.hentOppfolgingData(aktoerId).orElseThrow();

        assertThat(oppfolgingData.getManuell()).isFalse();
        pollElasticUntil(() -> elasticTestClient.hentBrukerFraElastic(aktoerId).getManuell_bruker() == null);
    }
}
