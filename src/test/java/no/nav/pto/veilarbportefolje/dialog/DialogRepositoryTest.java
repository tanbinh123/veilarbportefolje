package no.nav.pto.veilarbportefolje.dialog;


import no.nav.common.types.identer.AktorId;
import no.nav.pto.veilarbportefolje.util.DateUtils;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.ZonedDateTime;

import static no.nav.pto.veilarbportefolje.util.DateUtils.now;
import static no.nav.pto.veilarbportefolje.util.TestUtil.setupInMemoryDatabase;
import static org.assertj.core.api.Assertions.assertThat;


public class DialogRepositoryTest {


    private DialogRepository dialogRepository;

    @Before
    public void setup() {
        dialogRepository = new DialogRepository(new JdbcTemplate(setupInMemoryDatabase()));
    }

    private static final AktorId AKTOER_ID = AktorId.of("1111");

    @Test
    public void oppdaterDialogInfoForBruker_skal_sette_inn_i_tabell_og_vare_tilgjengelig_i_dialogview() {
        ZonedDateTime endringsDato = now();
        ZonedDateTime venteDato = now().minusMinutes(1000);

        dialogRepository.oppdaterDialogInfoForBruker(lagDialogData(venteDato, endringsDato));
        Dialogdata dialogFraDatabase = dialogRepository.retrieveDialogData(AKTOER_ID.toString()).get();
        verifiserData(venteDato, dialogFraDatabase, endringsDato);
    }

    private Dialogdata lagDialogData(ZonedDateTime venteDato, ZonedDateTime endringsDato) {
        return new Dialogdata()
                .setAktorId(AKTOER_ID.toString())
                .setTidspunktEldsteUbehandlede(venteDato)
                .setSisteEndring(endringsDato)
                .setTidspunktEldsteVentende(venteDato);
    }

    private void verifiserData(ZonedDateTime date, Dialogdata dialogFraDatabase, ZonedDateTime endringsDato) {
        assertThat(dialogFraDatabase.getTidspunktEldsteVentende()).isEqualTo(date);
        assertThat(dialogFraDatabase.getTidspunktEldsteUbehandlede()).isEqualTo(date);
        assertThat(dialogFraDatabase.getSisteEndring()).isEqualTo(endringsDato);
        assertThat(dialogFraDatabase.getAktorId()).isEqualTo(AKTOER_ID.toString());
    }

    @Test
    public void oppdaterDialogInfoForBruker_skal_oppdatere_tabell_og_vare_tilgjengelig_i_dialogview() {
        ZonedDateTime venteDato = DateUtils.now().minusSeconds(1);

        dialogRepository.oppdaterDialogInfoForBruker(lagDialogData(venteDato, venteDato));
        Dialogdata dialogFraDatabase = dialogRepository.retrieveDialogData(AKTOER_ID.toString()).get();
        verifiserData(venteDato, dialogFraDatabase, venteDato);

        ZonedDateTime nyEndringsDato = DateUtils.now();
        dialogRepository.oppdaterDialogInfoForBruker(lagDialogData(venteDato, nyEndringsDato));
        dialogFraDatabase = dialogRepository.retrieveDialogData(AKTOER_ID.toString()).get();
        verifiserData(venteDato, dialogFraDatabase, nyEndringsDato);
    }
}
