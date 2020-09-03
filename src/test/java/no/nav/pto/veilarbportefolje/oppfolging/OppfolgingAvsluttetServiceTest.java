package no.nav.pto.veilarbportefolje.oppfolging;

import io.vavr.control.Try;
import no.nav.common.client.aktorregister.AktorregisterClient;
import no.nav.pto.veilarbportefolje.TestUtil;
import no.nav.pto.veilarbportefolje.arbeidsliste.Arbeidsliste;
import no.nav.pto.veilarbportefolje.arbeidsliste.ArbeidslisteDTO;
import no.nav.pto.veilarbportefolje.arbeidsliste.ArbeidslisteRepository;
import no.nav.pto.veilarbportefolje.arbeidsliste.ArbeidslisteService;
import no.nav.pto.veilarbportefolje.database.BrukerRepository;
import no.nav.pto.veilarbportefolje.domene.Fnr;
import no.nav.pto.veilarbportefolje.kafka.EndToEndTest;
import no.nav.pto.veilarbportefolje.service.BrukerService;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static java.sql.Timestamp.from;
import static java.time.Instant.now;
import static no.nav.pto.veilarbportefolje.arbeidsliste.Arbeidsliste.Kategori.BLA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class OppfolgingAvsluttetServiceTest extends EndToEndTest {

    ArbeidslisteService arbeidslisteService;

    @BeforeClass
    public static void beforeClass() {
        final JdbcTemplate db = TestUtil.setupJdbc();

        final AktorregisterClient aktoerRegisterMock = mock(AktorregisterClient.class);

        new ArbeidslisteService(aktoerRegisterMock, new ArbeidslisteRepository(db), new BrukerService(new BrukerRepository(db), aktoerRegisterMock))
    }

    @Test
    public void skal_slette_arbeidsliste_når_bruker_ikke_lenger_er_under_oppfølging() {
        final Fnr fnr = Fnr.of("00000000000");
        final ArbeidslisteDTO dto = ArbeidslisteDTO.of(fnr, "", "", from(now()), BLA);
        arbeidslisteService.createArbeidsliste(dto);

        final Try<Arbeidsliste> arbeidsliste = arbeidslisteService.getArbeidsliste(fnr);
        assertThat(arbeidsliste.isSuccess()).isTrue();
        assertThat(arbeidsliste.get().getArbeidslisteAktiv()).isTrue();



    }

}
