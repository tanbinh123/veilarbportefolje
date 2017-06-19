package no.nav.fo.database;

import javaslang.control.Try;
import no.nav.fo.config.ApplicationConfigTest;
import no.nav.fo.domene.Arbeidsliste;
import no.nav.fo.domene.Fnr;
import no.nav.fo.provider.rest.arbeidsliste.ArbeidslisteData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;
import java.sql.Timestamp;
import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { ApplicationConfigTest.class})
public class ArbeidslisteRepositoryTest {

    @Inject
    private ArbeidslisteRepository repo;

    @Inject
    private JdbcTemplate jdbcTemplate;

    private ArbeidslisteData data;

    @Before
    public void setUp() throws Exception {
        data = new ArbeidslisteData(new Fnr("01010101010"))
                .setAktoerID("22222222")
                .setVeilederId("X11111")
                .setFrist(Timestamp.from(Instant.parse("2017-10-11T00:00:00Z")))
                .setKommentar("Dette er en kommentar");

        jdbcTemplate.execute("TRUNCATE TABLE ARBEIDSLISTE");

        Try<Boolean> result = repo.insertArbeidsliste(data);
        assertTrue(result.isSuccess());
    }

    @Test
    public void skalKunneHenteArbeidsliste() throws Exception {
        Try<Arbeidsliste> result = repo.retrieveArbeidsliste(data.getAktoerID());
        assertTrue(result.isSuccess());
        assertEquals(data.getVeilederId(), result.get().getVeilederId());
    }

    @Test
    public void skalOppdatereEksisterendeArbeidsliste() throws Exception {
        String expected = "TEST_ID";
        repo.updateArbeidsliste(data.setVeilederId(expected));

        Try<Arbeidsliste> result = repo.retrieveArbeidsliste(data.getAktoerID());
        assertTrue(result.isSuccess());
        assertEquals(expected, result.get().getVeilederId());
    }

    @Test
    public void skalSletteEksisterendeArbeidsliste() throws Exception {
        Try<Integer> result = repo.deleteArbeidsliste(data.getAktoerID());
        assertTrue(result.isSuccess());
    }

    @Test
    public void skalReturnereFailureVedSletting() throws Exception {
        Try<Integer> result = repo.deleteArbeidsliste("asdajsdklajsdkl");
        assertTrue(result.isFailure());
    }

    @Test
    public void skalReturnereFailureVedFeil() throws Exception {
        Try<Boolean> result = repo.insertArbeidsliste(data.setAktoerID(null));
        assertTrue(result.isFailure());
    }
}