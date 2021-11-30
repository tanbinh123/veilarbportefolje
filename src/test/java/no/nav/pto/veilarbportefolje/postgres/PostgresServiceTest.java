package no.nav.pto.veilarbportefolje.postgres;

import no.nav.arbeid.soker.registrering.UtdanningBestattSvar;
import no.nav.arbeid.soker.registrering.UtdanningGodkjentSvar;
import no.nav.arbeid.soker.registrering.UtdanningSvar;
import no.nav.common.types.identer.AktorId;
import no.nav.common.types.identer.Fnr;
import no.nav.pto.veilarbportefolje.aktiviteter.AktivitetStatus;
import no.nav.pto.veilarbportefolje.aktiviteter.AktivitetStatusRepositoryV2;
import no.nav.pto.veilarbportefolje.arbeidsliste.Arbeidsliste;
import no.nav.pto.veilarbportefolje.arbeidsliste.ArbeidslisteDTO;
import no.nav.pto.veilarbportefolje.arbeidsliste.ArbeidslisteRepositoryV2;
import no.nav.pto.veilarbportefolje.arenapakafka.ArenaDato;
import no.nav.pto.veilarbportefolje.arenapakafka.aktiviteter.TiltakRepositoryV3;
import no.nav.pto.veilarbportefolje.arenapakafka.arenaDTO.TiltakInnhold;
import no.nav.pto.veilarbportefolje.client.VeilarbVeilederClient;
import no.nav.pto.veilarbportefolje.config.ApplicationConfigTest;
import no.nav.pto.veilarbportefolje.dialog.DialogRepositoryV2;
import no.nav.pto.veilarbportefolje.dialog.Dialogdata;
import no.nav.pto.veilarbportefolje.domene.BrukereMedAntall;
import no.nav.pto.veilarbportefolje.domene.Filtervalg;
import no.nav.pto.veilarbportefolje.domene.Kjonn;
import no.nav.pto.veilarbportefolje.domene.value.PersonId;
import no.nav.pto.veilarbportefolje.domene.value.VeilederId;
import no.nav.pto.veilarbportefolje.oppfolging.OppfolgingRepositoryV2;
import no.nav.pto.veilarbportefolje.oppfolgingsbruker.OppfolgingsbrukerEntity;
import no.nav.pto.veilarbportefolje.oppfolgingsbruker.OppfolginsbrukerRepositoryV2;
import no.nav.pto.veilarbportefolje.registrering.DinSituasjonSvar;
import no.nav.pto.veilarbportefolje.util.VedtakstottePilotRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static no.nav.pto.veilarbportefolje.domene.Brukerstatus.*;
import static no.nav.pto.veilarbportefolje.util.DateUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SpringBootTest(classes = ApplicationConfigTest.class)
public class PostgresServiceTest {
    private final PostgresService postgresService;
    private final VeilarbVeilederClient veilarbVeilederClient;
    private final DialogRepositoryV2 dialogRepositoryV2;
    private final OppfolgingRepositoryV2 oppfolgingRepositoryV2;
    private final OppfolginsbrukerRepositoryV2 oppfolginsbrukerRepositoryV2;
    private final ArbeidslisteRepositoryV2 arbeidslisteRepositoryV2;
    private final AktivitetStatusRepositoryV2 aktivitetStatusRepositoryV2;
    private final TiltakRepositoryV3 tiltakRepositoryV3;

    private final String enhetId = "1234";

    private final static List<AktorId> randomAktorIds = List.of(getRandomAktorId(), getRandomAktorId(), getRandomAktorId(), getRandomAktorId());
    private final static List<Fnr> randomFnr = List.of(getRandomFnr(), getRandomFnr(), getRandomFnr(), getRandomFnr());
    private final List<Fnr> fixedFnr = List.of(Fnr.of("01091964488"), Fnr.of("09118714501"), Fnr.of("22098817732"));
    private final List<PersonId> randomPersonId = List.of(getRandomPersonId(), getRandomPersonId(), getRandomPersonId(), getRandomPersonId());

    @Autowired
    public PostgresServiceTest(@Qualifier("PostgresJdbc") JdbcTemplate db, DialogRepositoryV2 dialogRepositoryV2, OppfolgingRepositoryV2 oppfolgingRepositoryV2, OppfolginsbrukerRepositoryV2 oppfolginsbrukerRepositoryV2, ArbeidslisteRepositoryV2 arbeidslisteRepositoryV2, AktivitetStatusRepositoryV2 aktivitetStatusRepositoryV2, TiltakRepositoryV3 tiltakRepositoryV3) {
        this.dialogRepositoryV2 = dialogRepositoryV2;
        this.oppfolgingRepositoryV2 = oppfolgingRepositoryV2;
        this.oppfolginsbrukerRepositoryV2 = oppfolginsbrukerRepositoryV2;
        this.arbeidslisteRepositoryV2 = arbeidslisteRepositoryV2;
        this.aktivitetStatusRepositoryV2 = aktivitetStatusRepositoryV2;
        this.tiltakRepositoryV3 = tiltakRepositoryV3;
        VedtakstottePilotRequest vedtakstottePilotRequest = mock(VedtakstottePilotRequest.class);
        veilarbVeilederClient = mock(VeilarbVeilederClient.class);

        when(veilarbVeilederClient.hentVeilederePaaEnhet(any())).thenReturn(List.of("Z12345", "Z12346"));
        postgresService = new PostgresService(vedtakstottePilotRequest, db, veilarbVeilederClient);
    }

    @Before
    void setUp() {
        randomAktorIds.stream().forEach(aktorId -> oppfolgingRepositoryV2.settOppfolgingTilFalse(aktorId));
    }

    @Test
    public void sok_resulterer_i_ingen_brukere() {
        Filtervalg filtervalg = new Filtervalg().setFerdigfilterListe(List.of(UFORDELTE_BRUKERE));
        postgresService.hentBrukere("1234", null, null, null, filtervalg, 0, 10);
    }


    @Test
    public void sok_pa_utdanning() {
        Filtervalg filtervalg = new Filtervalg().setUtdanning(List.of(UtdanningSvar.GRUNNSKOLE))
                .setUtdanningGodkjent(List.of(UtdanningGodkjentSvar.JA))
                .setUtdanningBestatt(List.of(UtdanningBestattSvar.JA));
        postgresService.hentBrukere("1234", null, null, null, filtervalg, 0, 10);
    }


    @Test
    public void sok_pa_situasjon() {
        Filtervalg filtervalg = new Filtervalg().setRegistreringstype(List.of(DinSituasjonSvar.MISTET_JOBBEN));
        postgresService.hentBrukere("1234", null, null, null, filtervalg, 0, 10);
    }


    @Test
    public void sok_pa_tekst() {
        Filtervalg teskt = new Filtervalg().setNavnEllerFnrQuery("test");
        Filtervalg fnr = new Filtervalg().setNavnEllerFnrQuery("123");
        postgresService.hentBrukere("1234", null, null, null, teskt, 0, 10);
        postgresService.hentBrukere("1234", null, null, null, fnr, 0, 10);
    }

    @Test
    public void sok_pa_arbeidslista() {
        AktorId aktorId = randomAktorIds.get(0);
        Fnr fnr = randomFnr.get(0);
        lastOppBruker(fnr, aktorId, VeilederId.of("Z12345"));

        Filtervalg filtervalg = new Filtervalg().setFerdigfilterListe(List.of(MIN_ARBEIDSLISTE));
        BrukereMedAntall brukereMedAntall_pre = postgresService.hentBrukere(enhetId, null, null, null, filtervalg, 0, 10);
        assertThat(brukereMedAntall_pre.getAntall()).isEqualTo(0);

        arbeidslisteRepositoryV2.insertArbeidsliste(new ArbeidslisteDTO(fnr)
                .setAktorId(aktorId)
                .setVeilederId(VeilederId.of("X11111"))
                .setFrist(Timestamp.from(Instant.parse("2017-10-11T00:00:00Z")))
                .setKommentar("Dette er en kommentar")
                .setOverskrift("Dette er en overskrift")
                .setKategori(Arbeidsliste.Kategori.BLA));

        BrukereMedAntall brukereMedAntall_post = postgresService.hentBrukere(enhetId, null, null, null, filtervalg, 0, 10);
        assertThat(brukereMedAntall_post.getAntall()).isEqualTo(1);
        assertThat(brukereMedAntall_post.getBrukere().get(0).getArbeidsliste().getOverskrift()).isEqualTo("Dette er en overskrift");
    }

    @Test
    public void skal_filtrere_pa_kjonn() {
        when(veilarbVeilederClient.hentVeilederePaaEnhet(any())).thenReturn(List.of("Z12345", "Z12346"));
        lastOppBruker(fixedFnr.get(0), randomAktorIds.get(0), VeilederId.of("Z12345")); // Kvinne
        lastOppBruker(fixedFnr.get(2), randomAktorIds.get(1), VeilederId.of("Z12346")); // Mann

        Filtervalg filtervalg_kvinne = new Filtervalg().setFerdigfilterListe(List.of()).setKjonn(Kjonn.K);
        Filtervalg filtervalg_mann = new Filtervalg().setFerdigfilterListe(List.of()).setKjonn(Kjonn.M);

        BrukereMedAntall kvinne_respons = postgresService.hentBrukere(enhetId, null, null, null, filtervalg_kvinne, 0, 10);
        BrukereMedAntall mann_respons = postgresService.hentBrukere(enhetId, null, null, null, filtervalg_mann, 0, 10);

        assertThat(kvinne_respons.getAntall()).isEqualTo(1);
        assertThat(kvinne_respons.getBrukere().get(0).getKjonn()).isEqualTo("K");

        assertThat(mann_respons.getAntall()).isEqualTo(1);
        assertThat(mann_respons.getBrukere().get(0).getKjonn()).isEqualTo("M");
    }

    @Test
    public void skal_filtrere_pa_alder() {
        when(veilarbVeilederClient.hentVeilederePaaEnhet(any())).thenReturn(List.of("Z12345", "Z12346"));
        lastOppBruker(fixedFnr.get(0), randomAktorIds.get(0), VeilederId.of("Z12345")); // under_21
        lastOppBruker(fixedFnr.get(1), randomAktorIds.get(1), VeilederId.of("Z12346")); // Mann: 33

        Filtervalg alder_type_1 = new Filtervalg().setFerdigfilterListe(List.of()).setAlder(List.of("0-19"));
        Filtervalg alder_type_2 = new Filtervalg().setFerdigfilterListe(List.of()).setAlder(List.of("20-24", "30-39"));

        BrukereMedAntall alder_respons_type_1 = postgresService.hentBrukere(enhetId, null, null, "ikke_satt", alder_type_1, 0, 10);
        BrukereMedAntall alder_respons_type_2 = postgresService.hentBrukere(enhetId, null, null, "ikke_satt", alder_type_2, 0, 10);

        assertThat(alder_respons_type_1.getAntall()).isEqualTo(1);
        assertThat(alder_respons_type_2.getAntall()).isEqualTo(1);
    }

    @Test
    public void skal_filtrere_pa_fodselsdag() {
        when(veilarbVeilederClient.hentVeilederePaaEnhet(any())).thenReturn(List.of("Z12345", "Z12346"));
        lastOppBruker(fixedFnr.get(0), randomAktorIds.get(0), VeilederId.of("Z12345")); // 1 i maneden
        lastOppBruker(fixedFnr.get(1), randomAktorIds.get(1), VeilederId.of("Z12346")); // 9 i maneden

        Filtervalg alder_type_1 = new Filtervalg().setFerdigfilterListe(List.of()).setFodselsdagIMnd(List.of("1"));
        Filtervalg alder_type_2 = new Filtervalg().setFerdigfilterListe(List.of()).setFodselsdagIMnd(List.of("1", "9"));

        BrukereMedAntall alder_respons_type_1 = postgresService.hentBrukere(enhetId, null, null, null, alder_type_1, 0, 10);
        BrukereMedAntall alder_respons_type_2 = postgresService.hentBrukere(enhetId, null, null, null, alder_type_2, 0, 10);

        assertThat(alder_respons_type_1.getAntall()).isEqualTo(1);
        assertThat(alder_respons_type_2.getAntall()).isEqualTo(2);
        assertThat(alder_respons_type_1.getBrukere().get(0).getFnr()).isEqualTo(fixedFnr.get(0).toString());
    }

    @Test
    public void sok_pa_dialog() {
        lastOppBruker(randomFnr.get(0), randomAktorIds.get(0), VeilederId.of("Z12345"));

        ZonedDateTime venter_tidspunkt = now();
        dialogRepositoryV2.oppdaterDialogInfoForBruker(
                new Dialogdata()
                        .setAktorId(randomAktorIds.get(0).toString())
                        .setSisteEndring(now())
                        .setTidspunktEldsteVentende(venter_tidspunkt));

        Filtervalg filtervalg = new Filtervalg().setFerdigfilterListe(List.of(VENTER_PA_SVAR_FRA_BRUKER));

        BrukereMedAntall brukereMedAntall = postgresService.hentBrukere(enhetId, null, null, null, filtervalg, 0, 10);
        assertThat(brukereMedAntall.getAntall()).isEqualTo(1);
        assertThat(brukereMedAntall.getBrukere().get(0).getVenterPaSvarFraBruker()).isEqualTo(toLocalDateTimeOrNull(toTimestamp(venter_tidspunkt)));
    }

    @Test
    public void sok_pa_iavtaltAktivitet() {
        lastOppBruker(randomFnr.get(0), randomAktorIds.get(0), VeilederId.of("Z12345"));
        leggTilAktivitet(randomAktorIds.get(0), "behandling", toTimestamp(now().plusHours(2l)), toTimestamp(now().plusDays(1l)), true);


        lastOppBruker(randomFnr.get(1), randomAktorIds.get(1), VeilederId.of("Z12345"));
        leggTilAktivitet(randomAktorIds.get(1), "behandling", toTimestamp(now()), toTimestamp(now()), false);

        Filtervalg filtervalg = new Filtervalg().setFerdigfilterListe(List.of(I_AVTALT_AKTIVITET));
        BrukereMedAntall brukereMedAntall = postgresService.hentBrukere(enhetId, null, "descending", "iavtaltaktivitet", filtervalg, 0, 10);
        Assert.assertTrue(brukereMedAntall.getBrukere().size() == 1);
        Assert.assertFalse(brukereMedAntall.getBrukere().get(0).getFnr().equals(randomFnr.get(0)));
        Assert.assertFalse(brukereMedAntall.getBrukere().get(0).getAktiviteter().isEmpty());
    }

    @Test
    public void sok_pa_tiltaksTyper() {
        lastOppBruker(randomFnr.get(0), randomAktorIds.get(0), VeilederId.of("Z12345"));
        leggTilTiltak(randomAktorIds.get(0), randomPersonId.get(0), "INDOPPFAG", "2021-10-21 00:00:00", "2022-01-19 23:59:00");
        leggTilTiltak(randomAktorIds.get(0), randomPersonId.get(0), "INKLUTILS", "2021-10-22 00:00:00", "2021-11-22 00:00:00");

        lastOppBruker(randomFnr.get(1), randomAktorIds.get(1), VeilederId.of("Z12345"));
        leggTilTiltak(randomAktorIds.get(1), randomPersonId.get(1), "GRUPPEAMO", "2021-10-21 00:00:00", "2022-01-19 23:59:00");


        Filtervalg filtervalg = new Filtervalg().setTiltakstyper(List.of("INDOPPFAG", "NETTAMO"));
        BrukereMedAntall brukereMedAntall = postgresService.hentBrukere(enhetId, null, "descending", null, filtervalg, 0, 10);
        Assert.assertEquals(1, brukereMedAntall.getBrukere().size());
        Assert.assertEquals(brukereMedAntall.getBrukere().get(0).getFnr(), randomFnr.get(0).get());
    }

    @Test
    public void sok_pa_tiltaksTyper_og_iavtaltAktivitet() {
        lastOppBruker(randomFnr.get(0), randomAktorIds.get(0), VeilederId.of("Z12345"));

        Filtervalg filtervalg = new Filtervalg().setTiltakstyper(List.of("INDOPPFAG", "NETTAMO")).setFerdigfilterListe(List.of(I_AVTALT_AKTIVITET));
        BrukereMedAntall brukereMedAntall = postgresService.hentBrukere(enhetId, null, "descending", "iavtaltaktivitet", filtervalg, 0, 10);
    }

    @Test
    public void sok_pa_aktiviteterForenklet() {
        lastOppBruker(randomFnr.get(0), randomAktorIds.get(0), VeilederId.of("Z12345"));

        Filtervalg filtervalg = new Filtervalg().setAktiviteterForenklet(List.of("SOKEAVTALE", "STILLING"));
        BrukereMedAntall brukereMedAntall = postgresService.hentBrukere(enhetId, null, "descending", "", filtervalg, 0, 10);
    }

    private void lastOppBruker(Fnr fnr, AktorId aktorId, VeilederId veilederId) {
        oppfolgingRepositoryV2.settUnderOppfolging(aktorId, ZonedDateTime.now());
        oppfolgingRepositoryV2.settVeileder(aktorId, veilederId);
        oppfolginsbrukerRepositoryV2.leggTilEllerEndreOppfolgingsbruker(
                new OppfolgingsbrukerEntity(aktorId.get(), fnr.get(), null, null, "Testerson", "Testerson",
                        enhetId, null, null, null, null,
                        null, true, true, false, null, ZonedDateTime.now()));
    }

    private void leggTilAktivitet(AktorId aktorId, String aktivitetetType, Timestamp nesteUtlop, Timestamp nesteStart, boolean erAktiv) {
        aktivitetStatusRepositoryV2.upsertAktivitetTypeStatus(new AktivitetStatus()
                        .setAktoerid(aktorId)
                        .setAktivitetType(aktivitetetType)
                        .setAktiv(erAktiv)
                        .setNesteUtlop(nesteUtlop)
                        .setNesteStart(nesteStart)
                , aktivitetetType);
    }

    private void leggTilTiltak(AktorId aktorId, PersonId personId, String tiltaksType, String fraDato, String tilDato) {
        Random rnd = new Random();
        String randomAktivitetId = String.valueOf(Math.abs(rnd.nextInt()));

        tiltakRepositoryV3.upsert(new TiltakInnhold()
                        .setTiltakstype(tiltaksType)
                        .setAktivitetperiodeFra(new ArenaDato(fraDato))
                        .setAktivitetperiodeTil(new ArenaDato(tilDato))
                        .setAktivitetid(randomAktivitetId)
                        .setPersonId(personId.toInteger())
                , aktorId);
    }

    private static Fnr getRandomFnr() {
        Random rnd = new Random();
        long nextLong = Math.abs(rnd.nextLong());

        int twentyYears = 20 * 365;
        LocalDate randomDate = LocalDate.ofEpochDay(ThreadLocalRandom.current().nextInt(-twentyYears, twentyYears));
        DateTimeFormatter fnrFormat = DateTimeFormatter.ofPattern("ddMMyy");
        return Fnr.of(randomDate.format(fnrFormat) + String.valueOf(nextLong).substring(0, 4));
    }

    private static PersonId getRandomPersonId() {
        Random rnd = new Random();
        return PersonId.of(String.valueOf(Math.abs(rnd.nextInt())));
    }

    private static AktorId getRandomAktorId() {
        Random rnd = new Random();
        return AktorId.of(String.valueOf(Math.abs(rnd.nextInt())));
    }
}
