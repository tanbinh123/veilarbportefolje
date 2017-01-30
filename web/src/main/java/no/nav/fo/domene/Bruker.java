package no.nav.fo.domene;


import org.apache.solr.common.SolrDocument;

import java.util.*;

public class Bruker {
    private String fnr;
    private String fornavn;
    private String etternavn;
    private List<String> sikkerhetstiltak;
    private String diskresjonskode;
    private boolean egenAnsatt;
    private Map<String, String> veileder;

    public String getFnr() {
        return fnr;
    }

    public String getFornavn() {
        return fornavn;
    }

    public String getEtternavn() {
        return etternavn;
    }

    public String getDiskresjonskode() { return diskresjonskode; }

    public boolean getEgenAnsatt() { return egenAnsatt; }

    public List<String> getSikkerhetstiltak() { return sikkerhetstiltak; }

    public Map<String, String> getVeileder() { return veileder;}

    public Bruker() {
        sikkerhetstiltak = new ArrayList<>();
    }

    public static Bruker of(SolrDocument document) {
        Map<String, String> veileder = new HashMap<>();
        veileder.put("fornavn", "Arne");
        veileder.put("etternavn", "Olsen");
        veileder.put("ident", "X123456");

        return new Bruker()
                .withFnr((String) document.get("fodselsnr"))
                .withFornavn((String) document.get("fornavn"))
                .withEtternavn((String) document.get("etternavn"))
                .withVeileder(veileder);
    }

    public Bruker withFnr(String fnr) {
        this.fnr = fnr;
        return this;
    }

    public Bruker withFornavn(String fornavn) {
        this.fornavn = fornavn;
        return this;
    }

    public Bruker withEtternavn(String etternavn) {
        this.etternavn = etternavn;
        return this;
    }

    public Bruker withDiskresjonskode(String diskresjonskode) {
        this.diskresjonskode = diskresjonskode;
        return this;
    }

    public Bruker addSikkerhetstiltak(String sikkerhetstiltak) {
        this.sikkerhetstiltak.add(sikkerhetstiltak);
        return this;
    }

    public Bruker withVeileder(Map<String, String> veileder) {
        this.veileder = veileder;
        return this;
    }

    public Bruker erEgenAnsatt() {
        this.egenAnsatt = true;
        return this;
    }

    public static Comparator<Bruker> SORT_BY_LAST_NAME_ASCENDING = Comparator.comparing(Bruker::getEtternavn);

    public static Comparator<Bruker> SORT_BY_LAST_NAME_DESCENDING =
            (Bruker bruker1, Bruker bruker2) -> bruker2.getEtternavn().compareTo(bruker1.getEtternavn());


}
