package no.nav.pto.veilarbportefolje.registrering;

import no.nav.arbeid.soker.registrering.ArbeidssokerRegistrertEvent;
import no.nav.arbeid.soker.registrering.UtdanningBestattSvar;
import no.nav.arbeid.soker.registrering.UtdanningGodkjentSvar;
import no.nav.arbeid.soker.registrering.UtdanningSvar;
import no.nav.common.types.identer.AktorId;
import no.nav.pto.veilarbportefolje.util.DateUtils;
import no.nav.pto.veilarbportefolje.util.SingletonPostgresContainer;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Optional;

import static java.time.format.DateTimeFormatter.ISO_ZONED_DATE_TIME;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class RegistreringRepositoryV2Test {
    private JdbcTemplate db;
    private RegistreringRepositoryV2 registreringRepositoryV2;
    private static String AKTORID = "123456789";


    @Before
    public void setup() {
        db = SingletonPostgresContainer.init().createJdbcTemplate();
        registreringRepositoryV2 = new RegistreringRepositoryV2(db);
    }

    @Test
    public void skallSetteInBrukerSituasjon() {
        ArbeidssokerRegistrertEvent event = ArbeidssokerRegistrertEvent.newBuilder()
                .setAktorid(AKTORID)
                .setBrukersSituasjon("Permittert")
                .setUtdanning(UtdanningSvar.GRUNNSKOLE)
                .setUtdanningBestatt(UtdanningBestattSvar.INGEN_SVAR)
                .setUtdanningGodkjent(UtdanningGodkjentSvar.JA)
                .setRegistreringOpprettet(DateUtils.nowToStr())
                .build();

        registreringRepositoryV2.upsertBrukerRegistrering(event);

        Optional<ArbeidssokerRegistrertEvent> registrering = registreringRepositoryV2.hentBrukerRegistrering(AktorId.of(AKTORID));

        assertThat(registrering.isPresent()).isTrue();
        assertThat(registrering.orElseThrow(IllegalStateException::new)).isEqualTo(event);
    }

    @Test
    public void skallOppdatereBrukerSituasjon() {
        ArbeidssokerRegistrertEvent event1 = ArbeidssokerRegistrertEvent.newBuilder()
                .setAktorid(AKTORID)
                .setBrukersSituasjon("Permittert")
                .setUtdanning(UtdanningSvar.GRUNNSKOLE)
                .setUtdanningBestatt(UtdanningBestattSvar.INGEN_SVAR)
                .setUtdanningGodkjent(UtdanningGodkjentSvar.JA)
                .setRegistreringOpprettet(DateUtils.now().minusDays(4).format(ISO_ZONED_DATE_TIME))
                .build();

        registreringRepositoryV2.upsertBrukerRegistrering(event1);

        ArbeidssokerRegistrertEvent event2 = ArbeidssokerRegistrertEvent.newBuilder()
                .setAktorid(AKTORID)
                .setBrukersSituasjon("Hjemmekontor")
                .setUtdanning(UtdanningSvar.HOYERE_UTDANNING_1_TIL_4)
                .setUtdanningBestatt(UtdanningBestattSvar.INGEN_SVAR)
                .setUtdanningGodkjent(UtdanningGodkjentSvar.NEI)
                .setRegistreringOpprettet(DateUtils.nowToStr())
                .build();

        registreringRepositoryV2.upsertBrukerRegistrering(event2);

        Optional<ArbeidssokerRegistrertEvent> registrering = registreringRepositoryV2.hentBrukerRegistrering(AktorId.of(AKTORID));

        assertThat(registrering.isPresent()).isTrue();
        assertThat(registrering.orElseThrow(IllegalStateException::new)).isEqualTo(event2);
    }

    @Test
    public void skallOppdatereUtdanning() {
        ArbeidssokerRegistrertEvent event1 = ArbeidssokerRegistrertEvent.newBuilder()
                .setAktorid(AKTORID)
                .setBrukersSituasjon("Permittert")
                .setUtdanning(UtdanningSvar.GRUNNSKOLE)
                .setUtdanningBestatt(UtdanningBestattSvar.JA)
                .setUtdanningGodkjent(UtdanningGodkjentSvar.JA)
                .setRegistreringOpprettet(DateUtils.now().minusDays(4).format(ISO_ZONED_DATE_TIME))
                .build();
        registreringRepositoryV2.upsertBrukerRegistrering(event1);

        ArbeidssokerRegistrertEvent event2 = ArbeidssokerRegistrertEvent.newBuilder()
                .setAktorid(AKTORID)
                .setBrukersSituasjon("Permittert")
                .setUtdanning(UtdanningSvar.HOYERE_UTDANNING_5_ELLER_MER)
                .setUtdanningBestatt(UtdanningBestattSvar.NEI)
                .setUtdanningGodkjent(UtdanningGodkjentSvar.INGEN_SVAR)
                .setRegistreringOpprettet(DateUtils.nowToStr())
                .build();
        registreringRepositoryV2.upsertBrukerRegistrering(event2);

        Optional<ArbeidssokerRegistrertEvent> registrering = registreringRepositoryV2.hentBrukerRegistrering(AktorId.of(AKTORID));

        assertThat(registrering.orElseThrow(IllegalStateException::new)).isEqualTo(event2);
    }

}
