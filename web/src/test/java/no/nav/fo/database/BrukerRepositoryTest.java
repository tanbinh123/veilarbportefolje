package no.nav.fo.database;

import com.google.common.base.Joiner;
import no.nav.fo.config.ApplicationConfigTest;
import no.nav.fo.domene.Brukerdata;
import no.nav.fo.domene.KvartalMapping;
import no.nav.fo.domene.ManedMapping;
import no.nav.fo.domene.YtelseMapping;
import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrInputDocument;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Java6Assertions.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { ApplicationConfigTest.class})
public class BrukerRepositoryTest {

    @Inject
    private JdbcTemplate jdbcTemplate;

    @Inject
    private BrukerRepository brukerRepository;

    @Before
    public void setUp() {
        try {
            jdbcTemplate.execute(Joiner.on("\n").join(IOUtils.readLines(BrukerRepositoryTest.class.getResourceAsStream("/create-table-oppfolgingsbruker.sql"))));
            jdbcTemplate.execute(Joiner.on("\n").join(IOUtils.readLines(BrukerRepositoryTest.class.getResourceAsStream("/create-table-metadata.sql"))));
            jdbcTemplate.execute(Joiner.on("\n").join(IOUtils.readLines(BrukerRepositoryTest.class.getResourceAsStream("/insert-test-data-oppfolgingsbruker.sql"))));
            jdbcTemplate.execute(Joiner.on("\n").join(IOUtils.readLines(BrukerRepositoryTest.class.getResourceAsStream("/insert-test-metadata-sist-indeksert.sql"))));
            jdbcTemplate.execute(Joiner.on("\n").join(IOUtils.readLines(BrukerRepositoryTest.class.getResourceAsStream("/create-table-aktoerid-to-personid-mapping.sql"))));
            jdbcTemplate.execute(Joiner.on("\n").join(IOUtils.readLines(BrukerRepositoryTest.class.getResourceAsStream("/insert-aktoerid-to-personid-testdata.sql"))));
            jdbcTemplate.execute(Joiner.on("\n").join(IOUtils.readLines(BrukerRepositoryTest.class.getResourceAsStream("/create-table-bruker-data.sql"))));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        jdbcTemplate.execute("drop table oppfolgingsbruker");
        jdbcTemplate.execute("drop table metadata");
        jdbcTemplate.execute("drop table aktoerid_to_personid");
        jdbcTemplate.execute("drop table bruker_data");
    }

    @Test
    public void skalHenteUtAlleBrukereFraDatabasen() {
        List<Map<String, Object>> brukere = jdbcTemplate.queryForList(brukerRepository.retrieveBrukereSQL());

        assertThat(brukere.size()).isEqualTo(72);
    }

    @Test
    public void skalHaFolgendeFelterNaarHenterUtAlleBrukere() {
        Set<String> faktiskeDatabaseFelter = jdbcTemplate.queryForList(brukerRepository.retrieveBrukereSQL()).get(0).keySet();
        String[] skalHaDatabaseFelter = new String[] {"PERSON_ID", "FODSELSNR", "FORNAVN", "ETTERNAVN", "NAV_KONTOR",
                "FORMIDLINGSGRUPPEKODE", "ISERV_FRA_DATO", "KVALIFISERINGSGRUPPEKODE", "RETTIGHETSGRUPPEKODE",
                "HOVEDMAALKODE", "SIKKERHETSTILTAK_TYPE_KODE", "FR_KODE", "SPERRET_ANSATT", "ER_DOED", "DOED_FRA_DATO", "TIDSSTEMPEL", "VEILEDERIDENT", "YTELSE",
                "UTLOPSDATO", "UTLOPSDATOFASETT", "AAPMAXTID", "AAPMAXTIDFASETT"};

        assertThat(faktiskeDatabaseFelter).containsExactly(skalHaDatabaseFelter);
    }

    @Test
    public void skalHenteKunNyesteBrukereFraDatabasen() {
        List<Map<String, Object>> nyeBrukere = jdbcTemplate.queryForList(brukerRepository.retrieveOppdaterteBrukereSQL());
        jdbcTemplate.queryForList(brukerRepository.retrieveSistIndeksertSQL());
        assertThat(nyeBrukere.size()).isEqualTo(4);
    }

    @Test
    public void skalHaFolgendeFelterNaarHenterUtNyeBrukere() {
        Set<String> faktiskeDatabaseFelter = jdbcTemplate.queryForList(brukerRepository.retrieveOppdaterteBrukereSQL()).get(0).keySet();
        String[] skalHaDatabaseFelter = new String[] {"PERSON_ID", "FODSELSNR", "FORNAVN", "ETTERNAVN", "NAV_KONTOR",
                "FORMIDLINGSGRUPPEKODE", "ISERV_FRA_DATO", "KVALIFISERINGSGRUPPEKODE", "RETTIGHETSGRUPPEKODE",
                "HOVEDMAALKODE", "SIKKERHETSTILTAK_TYPE_KODE", "FR_KODE", "SPERRET_ANSATT", "ER_DOED", "DOED_FRA_DATO", "TIDSSTEMPEL", "VEILEDERIDENT",
                "YTELSE", "UTLOPSDATO", "UTLOPSDATOFASETT", "AAPMAXTID", "AAPMAXTIDFASETT"};

        assertThat(faktiskeDatabaseFelter).containsExactly(skalHaDatabaseFelter);
    }

    @Test
    public void skalKunHaEnCelleIIndekseringLogg() {
        List<Map<String, Object>> sistIndeksert = jdbcTemplate.queryForList("SELECT SIST_INDEKSERT FROM metadata");

        assertThat(sistIndeksert.size()).isEqualTo(1);
        assertThat(sistIndeksert.get(0).size()).isEqualTo(1);
    }

    @Test
    public void skalOppdatereSistIndeksertMedNyttTidsstempel() {
        Timestamp nyttTidsstempel = new Timestamp(System.currentTimeMillis());
        jdbcTemplate.update(brukerRepository.updateTidsstempelSQL(), nyttTidsstempel);

        Object sist_indeksert = jdbcTemplate.queryForList(brukerRepository.retrieveSistIndeksertSQL()).get(0).get("sist_indeksert");

        assertThat(sist_indeksert).isEqualTo(nyttTidsstempel);
    }

    @Test
    public void skalReturnerePersonidFraDB() {
        List<Map<String,Object>> mapping = brukerRepository.retrievePersonid("11111111");
        String personid = (String) mapping.get(0).get("PERSONID");
        Assertions.assertThat(personid).isEqualTo("222222");
    }

    @Test
    public void skalOppdatereOmBrukerFinnes() {
        Brukerdata brukerdata1 = brukerdata("aktoerid", "personid", "veielderid", LocalDateTime.now(), YtelseMapping.DAGPENGER_MED_PERMITTERING,
                LocalDateTime.now(), ManedMapping.MND1, LocalDateTime.now(), KvartalMapping.KV1);
        Brukerdata brukerdata2 = brukerdata("aktoerid", "personid", "veielderid2", LocalDateTime.now(), YtelseMapping.DAGPENGER_MED_PERMITTERING,
                LocalDateTime.now(), ManedMapping.MND1, LocalDateTime.now(), KvartalMapping.KV1);

        brukerRepository.insertOrUpdateBrukerdata(singletonList(brukerdata1), emptyList());
        brukerRepository.insertOrUpdateBrukerdata(singletonList(brukerdata1), singletonList("personid"));
        Brukerdata brukerdataAfterInsert = brukerRepository.retrieveBrukerdata(asList("personid")).get(0);
        assertThatBrukerdataIsEqual(brukerdata1, brukerdataAfterInsert);
        brukerRepository.insertOrUpdateBrukerdata(singletonList(brukerdata2), emptyList());
        Brukerdata brukerdataAfterUpdate = brukerRepository.retrieveBrukerdata(asList("personid")).get(0);
        assertThatBrukerdataIsEqual(brukerdata2, brukerdataAfterUpdate);

    }



    @Test
    public void skalInserteOmBrukerIkkeFinnes() {

        Brukerdata brukerdata = brukerdata("aktoerid", "personid", "veielderid", LocalDateTime.now(), YtelseMapping.DAGPENGER_MED_PERMITTERING,
                LocalDateTime.now(), ManedMapping.MND1, LocalDateTime.now(), KvartalMapping.KV1);

        brukerRepository.insertOrUpdateBrukerdata(singletonList(brukerdata), emptyList());

        Brukerdata brukerdataFromDb = brukerRepository.retrieveBrukerdata(asList("personid")).get(0);

        assertThatBrukerdataIsEqual(brukerdata, brukerdataFromDb);

    }



    @Test
    public void skalFiltrereBrukere() {
        List<SolrInputDocument> aktiveBrukere = brukerRepository.retrieveAlleBrukere();
        assertThat(aktiveBrukere.size()).isEqualTo(50);

        List<SolrInputDocument> oppdaterteAktiveBrukere = brukerRepository.retrieveOppdaterteBrukere();
        assertThat(oppdaterteAktiveBrukere.size()).isEqualTo(2);
    }

    private Brukerdata brukerdata(String aktoerid, String personId, String veileder, LocalDateTime tildeltTidspunkt, YtelseMapping ytelse,
                                  LocalDateTime utlopsdato, ManedMapping utlopsdatoFasett, LocalDateTime aapMaxtid, KvartalMapping aapMaxtidFasett) {
        return new Brukerdata()
                .setAktoerid(aktoerid)
                .setPersonid(personId)
                .setVeileder(veileder)
                .setTildeltTidspunkt(tildeltTidspunkt)
                .setAapMaxtidFasett(aapMaxtidFasett)
                .setAapMaxtid(aapMaxtid)
                .setUtlopsdatoFasett(utlopsdatoFasett)
                .setUtlopsdato(utlopsdato)
                .setYtelse(ytelse);

    }

    private void assertThatBrukerdataIsEqual(Brukerdata b1, Brukerdata b2) {
        assertThat(b1.getPersonid()).isEqualTo(b2.getPersonid());
        assertThat(b1.getAapMaxtid()).isEqualTo(b2.getAapMaxtid());
        assertThat(b1.getAktoerid()).isEqualTo(b2.getAktoerid());
        assertThat(b1.getAapMaxtidFasett()).isEqualTo(b2.getAapMaxtidFasett());
        assertThat(b1.getTildeltTidspunkt()).isEqualTo(b2.getTildeltTidspunkt());
        assertThat(b1.getVeileder()).isEqualTo(b2.getVeileder());
        assertThat(b1.getUtlopsdato()).isEqualTo(b2.getUtlopsdato());
        assertThat(b1.getUtlopsdatoFasett()).isEqualTo(b2.getUtlopsdatoFasett());
        assertThat(b1.getYtelse()).isEqualTo(b2.getYtelse());
    }

}
