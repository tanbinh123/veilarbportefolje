package no.nav.pto.veilarbportefolje.util;

import no.nav.common.types.identer.EnhetId;
import no.nav.pto.veilarbportefolje.config.ApplicationConfigTest;
import no.nav.pto.veilarbportefolje.domene.value.VeilederId;
import no.nav.pto.veilarbportefolje.elastic.ElasticIndexer;
import no.nav.pto.veilarbportefolje.elastic.ElasticServiceV2;
import no.nav.pto.veilarbportefolje.elastic.IndexName;
import no.nav.pto.veilarbportefolje.elastic.domene.OppfolgingsBruker;
import no.nav.pto.veilarbportefolje.service.UnleashService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

@ActiveProfiles("test")
@Import(ApplicationConfigTest.class)
@SpringBootTest
public abstract class EndToEndTest {

    @Autowired
    protected ElasticTestClient elasticTestClient;

    @Autowired
    protected TestDataClient testDataClient;

    @Autowired
    protected ElasticIndexer elasticIndexer;

    @Autowired
    protected ElasticServiceV2 elasticServiceV2;

    @Autowired
    protected IndexName indexName;

    @Autowired
    protected UnleashService unleashService;

    @BeforeEach
    void setUp() {
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(Optional.ofNullable(System.getenv("TZ")).orElse("Europe/Oslo")));
            elasticServiceV2.opprettNyIndeks(indexName.getValue());
        } catch (Exception e) {
            elasticServiceV2.slettIndex(indexName.getValue());
            elasticServiceV2.opprettNyIndeks(indexName.getValue());
        }
    }

    @AfterEach
    void tearDown() {
        elasticServiceV2.slettIndex(indexName.getValue());
    }

    public void populateElastic(EnhetId enhetId, VeilederId veilederId, String... aktoerIder) {
        List<OppfolgingsBruker> brukere = new ArrayList<>();
        for (String aktoerId : aktoerIder) {
            brukere.add(new OppfolgingsBruker()
                    .setAktoer_id(aktoerId)
                    .setOppfolging(true)
                    .setEnhet_id(enhetId.get())
                    .setVeileder_id(veilederId.getValue())
            );
        }

        brukere.forEach(bruker -> elasticTestClient.createUserInElastic(bruker));
    }
}
