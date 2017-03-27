package no.nav.fo.domene;

import org.apache.solr.common.SolrDocument;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class BrukerTest {
    @Test
    public void skalFylleUtAlleObligatoriskeFeltPaaBruker() throws Exception {
        Bruker bruker = Bruker.of(createSolrDocument("6"));

        assertThat(bruker.getFnr(), notNullValue());
        assertThat(bruker.getFornavn(), notNullValue());
        assertThat(bruker.getEtternavn(), notNullValue());
        assertThat(bruker.getVeilederId(), notNullValue());
        assertThat(bruker.isEgenAnsatt(), notNullValue());
        assertThat(bruker.getSikkerhetstiltak(), notNullValue());
        assertThat(bruker.getDiskresjonskode(), notNullValue());
        assertThat(bruker.isErDoed(), notNullValue());
    }

    @Test
    public void skalFiltrereBortDiskresjonskode() throws Exception {
        Bruker brukerKode5 = Bruker.of(createSolrDocument("5"));
        Bruker brukerKode6 = Bruker.of(createSolrDocument("6"));
        Bruker brukerKode7 = Bruker.of(createSolrDocument("7"));

        assertThat(brukerKode5.getDiskresjonskode(), nullValue());
        assertThat(brukerKode6.getDiskresjonskode()).isEqualTo("6");
        assertThat(brukerKode7.getDiskresjonskode()).isEqualTo("7");
    }

    @Test
    public void parseDato() throws Exception {
        String datoformat = "2017-03-30T22:00:00Z";
        LocalDateTime dato = Bruker.dato(datoformat);

        assertThat(dato.getYear()).isEqualTo(2017);
        assertThat(dato.getMonth()).isEqualTo(Month.MARCH);
        assertThat(dato.getDayOfMonth()).isEqualTo(30);
    }

    private SolrDocument createSolrDocument(String kode) {
        SolrDocument document = new SolrDocument();
        document.addField("fnr", "99999999999");
        document.addField("fornavn", "Rudolf");
        document.addField("etternavn", "Blodstrupmoen");
        document.addField("veileder_id", "XXXXXX");
        document.addField("utlopsdato", new Date());
        document.addField("egen_ansatt", false);
        document.addField("sikkerhetstiltak", "foo");
        document.addField("er_doed", false);
        document.addField("diskresjonskode", kode);
        document.addField("fodselsdag_i_mnd", 99);
        document.addField("fodselsdato", new Date());
        document.addField("kjonn", "K");
        document.addField("erInaktiv", false);

        return document;
    }
}

