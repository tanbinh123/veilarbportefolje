package no.nav.pto.veilarbportefolje.vedtakstotte;

import no.nav.pto.veilarbportefolje.domene.Hovedmal;
import no.nav.pto.veilarbportefolje.domene.Innsatsgruppe;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Java6Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;

import static no.nav.pto.veilarbportefolje.config.LocalJndiContextConfig.setupInMemoryDatabase;

public class VedtaksStatusRepositoryTest {

    private VedtakStatusRepository vedtakStatusRepository;

    private static String AKTORID = "123456789";
    private static long VEDTAKID = 1;

    @Before
    public void setup() {
        JdbcTemplate db = new JdbcTemplate(setupInMemoryDatabase());
        this.vedtakStatusRepository = new VedtakStatusRepository(db);
        vedtakStatusRepository.slettGamleVedtakOgUtkast(AKTORID);
    }


    @Test
    public void skallSetteInVedtakOgSletteUtkast() {
        insertVedtakIDB();
        List<KafkaVedtakStatusEndring> endringer = vedtakStatusRepository.hentVedtak(AKTORID);
        assertThat(endringer.size()).isEqualTo(1);
    }

    @Test
    public void skallOppdatereVedtak() {
        insertVedtakIDB();

        LocalDateTime time = LocalDateTime.now();
        KafkaVedtakStatusEndring kafkaVedtakStatusEndring = new KafkaVedtakStatusEndring()
                .setVedtakStatus(KafkaVedtakStatusEndring.KafkaVedtakStatus.SENDT_TIL_BESLUTTER)
                .setHovedmal(Hovedmal.SKAFFEA)
                .setInnsatsgruppe(Innsatsgruppe.VARIG)
                .setStatusEndretTidspunkt(time)
                .setAktorId(AKTORID)
                .setVedtakId(VEDTAKID);

        vedtakStatusRepository.updateVedtak(kafkaVedtakStatusEndring);
        List<KafkaVedtakStatusEndring> endringer = vedtakStatusRepository.hentVedtak(AKTORID);
        assertThat(endringer.size()).isEqualTo(1);
    }


    private void insertVedtakIDB() {
        LocalDateTime time = LocalDateTime.now();
        KafkaVedtakStatusEndring kafkaVedtakStatusEndring = new KafkaVedtakStatusEndring()
                .setVedtakStatus(KafkaVedtakStatusEndring.KafkaVedtakStatus.UTKAST_OPPRETTET)
                .setStatusEndretTidspunkt(time)
                .setAktorId(AKTORID)
                .setVedtakId(VEDTAKID)
                .setHovedmal(null)
                .setInnsatsgruppe(null);
        vedtakStatusRepository.insertVedtak(kafkaVedtakStatusEndring);
    }



}