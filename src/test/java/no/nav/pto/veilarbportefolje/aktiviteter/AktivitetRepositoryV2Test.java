package no.nav.pto.veilarbportefolje.aktiviteter;

import no.nav.common.types.identer.AktorId;
import no.nav.pto.veilarbportefolje.config.ApplicationConfigTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.ZonedDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = ApplicationConfigTest.class)
public class AktivitetRepositoryV2Test {

    @Autowired
    private AktivitetRepositoryV2 aktivitetRepositoryV2;

    @Test
    public void getAktorId_skal_vaere_tom_ved_manglende_aktoer_id() {
        assertThat(aktivitetRepositoryV2.getAktorId("finnes_ikke")).isEqualTo(Optional.empty());
    }

    @Test
    public void skal_hente_aktoerid_for_aktivitet() {
        AktorId expectedAktorId = AktorId.of("aktoer_id_test_1");
        String aktivitetId = "aktivitet_id_test_1";

        KafkaAktivitetMelding aktivitet1 = new KafkaAktivitetMelding()
                .setAktivitetId(aktivitetId)
                .setAktivitetType(KafkaAktivitetMelding.AktivitetTypeData.IJOBB)
                .setAktorId("aktoer_id_test_1")
                .setAvtalt(false)
                .setFraDato(ZonedDateTime.parse("2017-03-03T10:10:10+02:00"))
                .setTilDato(ZonedDateTime.parse("2017-12-03T10:10:10+02:00"))
                .setEndretDato(ZonedDateTime.parse("2017-02-03T10:10:10+02:00"))
                .setAktivitetStatus(KafkaAktivitetMelding.AktivitetStatus.PLANLAGT);

        KafkaAktivitetMelding aktivitet2 = new KafkaAktivitetMelding()
                .setAktivitetId("aktivitet_id_test_2")
                .setAktorId("aktoer_id_test_2")
                .setAktivitetType(KafkaAktivitetMelding.AktivitetTypeData.IJOBB)
                .setAvtalt(false)
                .setFraDato(ZonedDateTime.parse("2017-03-03T10:10:10+02:00"))
                .setTilDato(ZonedDateTime.parse("2017-12-03T10:10:10+02:00"))
                .setEndretDato(ZonedDateTime.parse("2017-02-03T10:10:10+02:00"))
                .setAktivitetStatus(KafkaAktivitetMelding.AktivitetStatus.PLANLAGT);

        KafkaAktivitetMelding aktivitet3 = new KafkaAktivitetMelding()
                .setAktivitetId(aktivitetId)
                .setAktorId("aktoer_id_test_1")
                .setAktivitetType(KafkaAktivitetMelding.AktivitetTypeData.IJOBB)
                .setAktorId("aktoer_id_test_1")
                .setAvtalt(false)
                .setFraDato(ZonedDateTime.parse("2017-03-03T10:10:10+02:00"))
                .setTilDato(ZonedDateTime.parse("2017-12-03T10:10:10+02:00"))
                .setEndretDato(ZonedDateTime.parse("2017-02-03T10:10:10+02:00"))
                .setAktivitetStatus(KafkaAktivitetMelding.AktivitetStatus.PLANLAGT);

        aktivitetRepositoryV2.upsertAktivitet(aktivitet1);
        aktivitetRepositoryV2.upsertAktivitet(aktivitet2);
        aktivitetRepositoryV2.upsertAktivitet(aktivitet3);

        AktorId actualAktorId = aktivitetRepositoryV2.getAktorId(aktivitetId).get();

        assertThat(actualAktorId).isEqualTo(expectedAktorId);
    }
/*
    @Test
    public void skalSetteInnAktivitet() {
        KafkaAktivitetMelding aktivitet = new KafkaAktivitetMelding()
                .setAktivitetId("aktivitetid")
                .setAktivitetType(KafkaAktivitetMelding.AktivitetTypeData.EGEN)
                .setAktorId("aktoerid")
                .setAvtalt(false)
                .setFraDato(ZonedDateTime.parse("2017-03-03T10:10:10+02:00"))
                .setTilDato(ZonedDateTime.parse("2017-12-03T10:10:10+02:00"))
                .setEndretDato(ZonedDateTime.parse("2017-02-03T10:10:10+02:00"))
                .setAktivitetStatus(KafkaAktivitetMelding.AktivitetStatus.GJENNOMFORES);

        aktivitetRepositoryV2.upsertAktivitet(aktivitet);

        //Map<String, Object> aktivitetFraDB = jdbcTemplate.queryForList("select * from aktiviteter where aktivitetid='aktivitetid'").get(0);

        String status = (String) aktivitetFraDB.get("status");
        String type = (String) aktivitetFraDB.get("aktivitettype");

        assertThat(status).isEqualToIgnoringCase(KafkaAktivitetMelding.AktivitetStatus.GJENNOMFORES.name());
        assertThat(type).isEqualToIgnoringCase(KafkaAktivitetMelding.AktivitetTypeData.EGEN.name());
    }

    @Test
    public void skalOppdatereAktivitet() {
        KafkaAktivitetMelding aktivitet1 = new KafkaAktivitetMelding()
                .setAktivitetId("aktivitetid")
                .setAktivitetType(KafkaAktivitetMelding.AktivitetTypeData.SOKEAVTALE)
                .setAktorId("aktoerid")
                .setAvtalt(false)
                .setFraDato(ZonedDateTime.parse("2017-03-03T10:10:10+02:00"))
                .setTilDato(ZonedDateTime.parse("2017-12-03T10:10:10+02:00"))
                .setEndretDato(ZonedDateTime.parse("2017-02-03T10:10:10+02:00"))
                .setAktivitetStatus(KafkaAktivitetMelding.AktivitetStatus.BRUKER_ER_INTERESSERT);

        KafkaAktivitetMelding aktivitet2 = new KafkaAktivitetMelding()
                .setAktivitetId("aktivitetid")
                .setAktivitetType(KafkaAktivitetMelding.AktivitetTypeData.SOKEAVTALE)
                .setAktorId("aktoerid")
                .setAvtalt(false)
                .setFraDato(ZonedDateTime.parse("2017-03-03T10:10:10+02:00"))
                .setTilDato(ZonedDateTime.parse("2017-12-03T10:10:10+02:00"))
                .setEndretDato(ZonedDateTime.parse("2017-02-03T10:10:10+02:00"))
                .setAktivitetStatus(KafkaAktivitetMelding.AktivitetStatus.FULLFORT);

        aktivitetDAO.upsertAktivitet(aktivitet1);
        aktivitetDAO.upsertAktivitet(aktivitet2);

        String status = (String) jdbcTemplate.queryForList("select * from aktiviteter where aktivitetid='aktivitetid'").get(0).get("status");

        assertThat(status).isEqualToIgnoringCase(KafkaAktivitetMelding.AktivitetStatus.FULLFORT.name());

    }


    @Test
    public void skalHenteDistinkteAktorider() {

        KafkaAktivitetMelding aktivitet1 = new KafkaAktivitetMelding()
                .setAktivitetId("id1")
                .setAktivitetType(KafkaAktivitetMelding.AktivitetTypeData.EGEN)
                .setAktorId("aktoerid")
                .setAvtalt(true)
                .setEndretDato(ZonedDateTime.parse("2017-02-03T10:10:10+02:00"))
                .setAktivitetStatus(KafkaAktivitetMelding.AktivitetStatus.PLANLAGT);

        KafkaAktivitetMelding aktivitet2 = new KafkaAktivitetMelding()
                .setAktivitetId("id2")
                .setAktivitetType(KafkaAktivitetMelding.AktivitetTypeData.EGEN)
                .setAktorId("aktoerid")
                .setAvtalt(true)
                .setEndretDato(ZonedDateTime.parse("2017-02-03T10:10:10+02:00"))
                .setAktivitetStatus(KafkaAktivitetMelding.AktivitetStatus.PLANLAGT);

        aktivitetDAO.upsertAktivitet(aktivitet1);
        aktivitetDAO.upsertAktivitet(aktivitet2);

        assertThat(aktivitetDAO.getDistinctAktorIdsFromAktivitet()).containsExactly("aktoerid");
    }

    @Test
    public void skalHenteListeMedAktiviteterForAktorid() {
        KafkaAktivitetMelding aktivitet1 = new KafkaAktivitetMelding()
                .setAktivitetId("id1")
                .setAktivitetType(KafkaAktivitetMelding.AktivitetTypeData.EGEN)
                .setAktorId("aktoerid")
                .setAvtalt(true)
                .setEndretDato(ZonedDateTime.parse("2017-02-03T10:10:10+02:00"))
                .setAktivitetStatus(KafkaAktivitetMelding.AktivitetStatus.PLANLAGT);

        KafkaAktivitetMelding aktivitet2 = new KafkaAktivitetMelding()
                .setAktivitetId("id2")
                .setAktivitetType(KafkaAktivitetMelding.AktivitetTypeData.EGEN)
                .setAktorId("aktoerid")
                .setAvtalt(true)
                .setEndretDato(ZonedDateTime.parse("2017-02-03T10:10:10+02:00"))
                .setAktivitetStatus(KafkaAktivitetMelding.AktivitetStatus.PLANLAGT);;

        aktivitetDAO.upsertAktivitet(asList(aktivitet1, aktivitet2));

        AktoerAktiviteter aktoerAktiviteter = aktivitetDAO.getAktiviteterForAktoerid(AktorId.of("aktoerid"));

        assertThat(aktoerAktiviteter.getAktiviteter().size()).isEqualTo(2);
        assertThat(aktoerAktiviteter.getAktoerid()).isEqualTo("aktoerid");
    }

    @Test
    public void skalInserteBatchAvAktivitetstatuser() {
        List<AktivitetStatus> statuser = new ArrayList<>();

        statuser.add(new AktivitetStatus()
                .setPersonid(PersonId.of("pid1"))
                .setAktoerid( AktorId.of("aid1"))
                .setAktivitetType("a1")
                .setAktiv(true)
                .setNesteStart(new Timestamp(0))
                .setNesteUtlop( new Timestamp(0)));

        statuser.add(new AktivitetStatus()
                .setPersonid(PersonId.of("pid2"))
                .setAktoerid( AktorId.of("aid2"))
                .setAktivitetType("a2")
                .setAktiv(true)
                .setNesteStart(new Timestamp(0))
                .setNesteUtlop( new Timestamp(0)));

        aktivitetDAO.insertAktivitetstatuser(statuser);
        assertThat(jdbcTemplate.queryForList("SELECT * FROM BRUKERSTATUS_AKTIVITETER").size()).isEqualTo(2);
    }

    @Test
    public void skalReturnereTomtMapDersomIngenBrukerHarAktivitetstatusIDB() {
        assertThat(aktivitetDAO.getAktivitetstatusForBrukere(asList(PersonId.of("personid")))).isEqualTo(new HashMap<>());
    }

    @Test
    public void skalHenteBrukertiltakForListeAvFnr() {
        Fnr fnr1 = Fnr.ofValidFnr("11111111111");
        Fnr fnr2 = Fnr.ofValidFnr("22222222222");

        List<Brukertiltak> brukertiltak = aktivitetDAO.hentBrukertiltak(asList(fnr1, fnr2));

        assertThat(brukertiltak.get(0).getTiltak().equals("T1")).isTrue();
        assertThat(brukertiltak.get(1).getTiltak().equals("T2")).isTrue();
        assertThat(brukertiltak.get(2).getTiltak().equals("T1")).isTrue();
    }

    @Test
    public void skalHaRiktigVersionLogikk(){
        KafkaAktivitetMelding aktivitet_i_database = new KafkaAktivitetMelding()
                .setVersion(2L)
                .setAktivitetId("aktivitetid")
                .setAktivitetType(KafkaAktivitetMelding.AktivitetTypeData.SOKEAVTALE)
                .setAktorId("aktoerid")
                .setAvtalt(false)
                .setFraDato(ZonedDateTime.parse("2017-03-03T10:10:10+02:00"))
                .setTilDato(ZonedDateTime.parse("2017-12-03T10:10:10+02:00"))
                .setEndretDato(ZonedDateTime.parse("2017-02-03T10:10:10+02:00"))
                .setAktivitetStatus(KafkaAktivitetMelding.AktivitetStatus.BRUKER_ER_INTERESSERT);

        KafkaAktivitetMelding aktivitet_gammel = new KafkaAktivitetMelding()
                .setVersion(1L)
                .setAktivitetId("aktivitetid")
                .setAktivitetType(KafkaAktivitetMelding.AktivitetTypeData.SOKEAVTALE)
                .setAktorId("aktoerid")
                .setAvtalt(false)
                .setFraDato(ZonedDateTime.parse("2017-03-03T10:10:10+02:00"))
                .setTilDato(ZonedDateTime.parse("2017-12-03T10:10:10+02:00"))
                .setEndretDato(ZonedDateTime.parse("2017-02-03T10:10:10+02:00"))
                .setAktivitetStatus(KafkaAktivitetMelding.AktivitetStatus.BRUKER_ER_INTERESSERT);


        KafkaAktivitetMelding aktivitet_ny = new KafkaAktivitetMelding()
                .setVersion(3L)
                .setAktivitetId("aktivitetid")
                .setAktivitetType(KafkaAktivitetMelding.AktivitetTypeData.SOKEAVTALE)
                .setAktorId("aktoerid")
                .setAvtalt(false)
                .setFraDato(ZonedDateTime.parse("2017-03-03T10:10:10+02:00"))
                .setTilDato(ZonedDateTime.parse("2017-12-03T10:10:10+02:00"))
                .setEndretDato(ZonedDateTime.parse("2017-02-03T10:10:10+02:00"))
                .setAktivitetStatus(KafkaAktivitetMelding.AktivitetStatus.BRUKER_ER_INTERESSERT);


        assertThat(aktivitetDAO.erNyVersjonAvAktivitet(aktivitet_i_database)).isTrue();

        aktivitetDAO.upsertAktivitet(aktivitet_i_database);
        assertThat(aktivitetDAO.erNyVersjonAvAktivitet(aktivitet_gammel)).isFalse();
        assertThat(aktivitetDAO.erNyVersjonAvAktivitet(aktivitet_ny)).isTrue();
    }*/
}
