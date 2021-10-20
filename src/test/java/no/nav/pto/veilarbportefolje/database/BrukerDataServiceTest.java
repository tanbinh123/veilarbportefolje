package no.nav.pto.veilarbportefolje.database;

import no.nav.common.types.identer.AktorId;
import no.nav.pto.veilarbportefolje.aktiviteter.AktivitetDAO;
import no.nav.pto.veilarbportefolje.aktiviteter.AktivitetStatusRepositoryV2;
import no.nav.pto.veilarbportefolje.aktiviteter.AktiviteterRepositoryV2;
import no.nav.pto.veilarbportefolje.arenapakafka.aktiviteter.GruppeAktivitetRepository;
import no.nav.pto.veilarbportefolje.arenapakafka.aktiviteter.GruppeAktivitetRepositoryV2;
import no.nav.pto.veilarbportefolje.arenapakafka.aktiviteter.TiltakRepository;
import no.nav.pto.veilarbportefolje.arenapakafka.aktiviteter.TiltakRepositoryV2;
import no.nav.pto.veilarbportefolje.config.ApplicationConfigTest;
import no.nav.pto.veilarbportefolje.domene.Brukerdata;
import no.nav.pto.veilarbportefolje.domene.value.PersonId;
import no.nav.sbl.sql.SqlUtils;
import no.nav.sbl.sql.where.WhereClause;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

import static java.util.concurrent.ThreadLocalRandom.current;
import static no.nav.pto.veilarbportefolje.util.DateUtils.now;
import static no.nav.pto.veilarbportefolje.util.DateUtils.toTimestamp;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;


@SpringBootTest(classes = ApplicationConfigTest.class)
public class BrukerDataServiceTest {
    private final JdbcTemplate jdbcTemplate;
    private final BrukerDataService brukerDataService;
    private final BrukerRepository brukerRepository;

    private final AktorId aktorId = AktorId.of("1000123");
    private final PersonId personId = PersonId.of("123");


    @Autowired
    public BrukerDataServiceTest(AktivitetDAO aktivitetDAO, JdbcTemplate jdbcTemplate, TiltakRepository tiltakRepository, GruppeAktivitetRepository gruppeAktivitetRepository, BrukerDataRepository brukerDataRepository, BrukerRepository brukerRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.brukerRepository = brukerRepository;
        brukerDataService = new BrukerDataService(aktivitetDAO, tiltakRepository, mock(TiltakRepositoryV2.class), gruppeAktivitetRepository, mock(GruppeAktivitetRepositoryV2.class), brukerDataRepository, mock(AktiviteterRepositoryV2.class), mock(AktivitetStatusRepositoryV2.class));
    }

    @BeforeEach
    public void reset() {
        jdbcTemplate.execute("truncate table " + Table.OPPFOLGINGSBRUKER.TABLE_NAME);
        jdbcTemplate.execute("truncate table " + Table.OPPFOLGING_DATA.TABLE_NAME);
        jdbcTemplate.execute("truncate table " + Table.AKTOERID_TO_PERSONID.TABLE_NAME);
        jdbcTemplate.execute("truncate table " + Table.AKTIVITETER.TABLE_NAME);
        jdbcTemplate.execute("truncate table " + Table.BRUKER_DATA.TABLE_NAME);
        jdbcTemplate.execute("truncate table " + Table.BRUKERTILTAK_V2.TABLE_NAME);
        jdbcTemplate.execute("truncate table BRUKERSTATUS_AKTIVITETER");
    }

    @Test
    public void skalOppdatereBrukerData() {
        Timestamp enUkeSiden = toTimestamp(now().minusDays(7));
        Timestamp toUkerSiden = toTimestamp(now().minusDays(14));

        Timestamp enUkeTil = toTimestamp(now().plusDays(7));
        Timestamp toUkerTil = toTimestamp(now().plusDays(14));
        Timestamp treUkerTil = toTimestamp(now().plusDays(21));

        insertAktivitet(toUkerSiden, enUkeSiden);
        insertAktivitet(enUkeTil, toUkerTil);
        insertTiltak(toUkerTil, treUkerTil);

        brukerDataService.oppdaterAktivitetBrukerData(aktorId, personId);
        Brukerdata brukerdata = brukerRepository.retrieveBrukerdata(List.of(personId.getValue())).get(0);

        assertThat(brukerdata.getNyesteUtlopteAktivitet()).isEqualTo(enUkeSiden);

        assertThat(brukerdata.getAktivitetStart()).isEqualTo(enUkeTil);
        assertThat(brukerdata.getNesteAktivitetStart()).isEqualTo(toUkerTil);
        assertThat(brukerdata.getForrigeAktivitetStart()).isEqualTo(toUkerSiden);
    }

    private void insertTiltak(Timestamp startDato, Timestamp tilDato) {
        String id = String.valueOf(current().nextInt());
        SqlUtils.upsert(jdbcTemplate, Table.BRUKERTILTAK_V2.TABLE_NAME)
                .set(Table.BRUKERTILTAK_V2.AKTIVITETID, id)
                .set(Table.BRUKERTILTAK_V2.PERSONID, personId.getValue())
                .set(Table.BRUKERTILTAK_V2.AKTOERID, aktorId.get())
                .set(Table.BRUKERTILTAK_V2.TILTAKSKODE, "GRUPPEAMO")
                .set(Table.BRUKERTILTAK_V2.FRADATO, startDato)
                .set(Table.BRUKERTILTAK_V2.TILDATO, tilDato)
                .where(WhereClause.equals(Table.BRUKERTILTAK_V2.AKTIVITETID, id))
                .execute();
    }

    private void insertAktivitet(Timestamp startDato, Timestamp tilDato) {
        String id = String.valueOf(current().nextInt());
        SqlUtils.upsert(jdbcTemplate, Table.AKTIVITETER.TABLE_NAME)
                .set(Table.AKTIVITETER.AKTOERID, aktorId.get())
                .set(Table.AKTIVITETER.AKTIVITETTYPE, "egen")
                .set(Table.AKTIVITETER.AVTALT, true)
                .set(Table.AKTIVITETER.FRADATO, startDato)
                .set(Table.AKTIVITETER.TILDATO, tilDato)
                .set(Table.AKTIVITETER.OPPDATERTDATO, Timestamp.valueOf(LocalDateTime.now()))
                .set(Table.AKTIVITETER.STATUS, "GJENNOMFORES".toLowerCase())
                .set(Table.AKTIVITETER.VERSION, 1)
                .set(Table.AKTIVITETER.AKTIVITETID, id)
                .where(WhereClause.equals(Table.AKTIVITETER.AKTIVITETID, id))
                .execute();
    }
}
