package no.nav.fo.service;

import io.vavr.collection.List;
import io.vavr.control.Try;
import no.nav.dialogarena.aktor.AktorService;
import no.nav.fo.config.ApplicationConfigTest;
import no.nav.fo.domene.AktoerId;
import no.nav.fo.domene.Fnr;
import no.nav.fo.domene.PersonId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;

import static java.util.Optional.ofNullable;
import static no.nav.fo.database.BrukerRepository.OPPFOLGINGSBRUKER;
import static no.nav.fo.mock.AktoerServiceMock.*;
import static no.nav.fo.util.sql.SqlUtils.insert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ApplicationConfigTest.class})
public class AktoerServiceTest {

    @Inject
    private AktoerService aktoerService;

    @Inject
    private AktorService aktorService;

    @Inject
    private JdbcTemplate db;

    private String FNR_FRA_SOAP_TJENESTE = "11111111111";
    private String AKTOERID_FRA_SOAP_TJENESTE = "2222";


    @Before
    public void setUp() throws Exception {
        when(aktorService.getAktorId(anyString())).thenReturn(ofNullable(AKTOERID_FRA_SOAP_TJENESTE));
        when(aktorService.getFnr(anyString())).thenReturn(ofNullable(FNR_FRA_SOAP_TJENESTE));
    }

    @After
    public void tearDown() throws Exception {
        db.execute("TRUNCATE TABLE BRUKER_DATA");
        db.execute("TRUNCATE TABLE OPPFOLGINGSBRUKER");
        db.execute("TRUNCATE TABLE AKTOERID_TO_PERSONID");
    }

    @Test
    public void skalFinnePersonIdFraDatabase() throws Exception {
        AktoerId aktoerId = AktoerId.of("111");
        PersonId personId = PersonId.of("222");
        int updated =
                insert(db, "AKTOERID_TO_PERSONID")
                        .value("PERSONID", personId.toString())
                        .value("AKTOERID", aktoerId.toString())
                        .execute();

        assertTrue(updated > 0);

        Try<PersonId> result = aktoerService.hentPersonidFraAktoerid(aktoerId);
        verify(aktorService, never()).getFnr(any());
        assertTrue(result.isSuccess());
        assertEquals(personId, result.get());
    }

    @Test
    public void skalFinnePersonIdViaSoapTjeneste() throws Exception {
        AktoerId aktoerId = AktoerId.of("111");
        PersonId personId = PersonId.of("222");

        int updated =
                insert(db, OPPFOLGINGSBRUKER)
                        .value("PERSON_ID", personId.toString())
                        .value("FODSELSNR", FNR_FRA_SOAP_TJENESTE)
                        .execute();

        assertTrue(updated > 0);

        Try<PersonId> result = aktoerService.hentPersonidFraAktoerid(aktoerId);
        assertTrue(result.isSuccess());
        assertEquals(personId, result.get());
    }

    @Test
    public void skalFortsetteVedFeil() throws Exception {
        List<String> aktoerIds = List.of("1", "2", "3", "4");
        int resultLength = aktoerIds
                .map(AktoerId::of)
                .map(aktoerService::hentPersonidFraAktoerid)
                .length();

        assertTrue(resultLength == aktoerIds.length());
    }

    @Test
    public void skalHenteAktoerIdFraPersonId() throws Exception {
        PersonId personId = PersonId.of(PERSON_ID);
        AktoerId aktoerId = AktoerId.of(AKTOER_ID);
        int updated = insert(db, "AKTOERID_TO_PERSONID")
                .value("AKTOERID", aktoerId.toString())
                .value("PERSONID", personId.toString())
                .execute();

        assertTrue(updated > 0);

        Try<AktoerId> result = aktoerService.hentAktoeridFraPersonid(personId);
        assertTrue(result.isSuccess());
        assertEquals(aktoerId, result.get());
    }

    @Test
    public void skalHenteAktoerIdFraFnrViaSoap() throws Exception {
        Fnr fnr = new Fnr(FNR);
        Try<AktoerId> aktoerId = aktoerService.hentAktoeridFraFnr(fnr);
        assertTrue(aktoerId.isSuccess());
        assertEquals(AktoerId.of(AKTOERID_FRA_SOAP_TJENESTE), aktoerId.get());
    }

    @Test
    public void skalIKKEHenteFnrFraAktoerIdFraDb() throws Exception {
        AktoerId aktoerId = AktoerId.of(AKTOER_ID);

        Try<Fnr> result = aktoerService.hentFnrFraAktoerid(aktoerId);
        assertTrue(result.isSuccess());
        assertEquals(new Fnr(FNR_FRA_SOAP_TJENESTE), result.get());
    }
}