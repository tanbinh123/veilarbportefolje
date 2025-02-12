package no.nav.pto.veilarbportefolje.domene;

import lombok.Data;
import lombok.experimental.Accessors;
import no.nav.arbeid.soker.registrering.UtdanningBestattSvar;
import no.nav.arbeid.soker.registrering.UtdanningGodkjentSvar;
import no.nav.arbeid.soker.registrering.UtdanningSvar;
import no.nav.pto.veilarbportefolje.registrering.DinSituasjonSvar;
import no.nav.pto.veilarbportefolje.sisteendring.SisteEndringsKategori;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BinaryOperator;

@Data()
@Accessors(chain = true)
public class Filtervalg {
    public Brukerstatus brukerstatus;
    public List<Brukerstatus> ferdigfilterListe;
    public YtelseFilter ytelse;
    public List<String> alder = new ArrayList<>();
    public Kjonn kjonn;
    public List<String> fodselsdagIMnd = new ArrayList<>();
    public List<Innsatsgruppe> innsatsgruppe = new ArrayList<>();
    public List<Hovedmal> hovedmal = new ArrayList<>();
    public List<Formidlingsgruppe> formidlingsgruppe = new ArrayList<>();
    public List<Servicegruppe> servicegruppe = new ArrayList<>();
    public List<Rettighetsgruppe> rettighetsgruppe = new ArrayList<>();
    public List<String> veiledere = new ArrayList<>();
    public Map<String, AktivitetFiltervalg> aktiviteter = new HashMap<>();
    public List<String> tiltakstyper = new ArrayList<>();
    public List<ManuellBrukerStatus> manuellBrukerStatus = new ArrayList<>();
    public String navnEllerFnrQuery;
    public List<DinSituasjonSvar> registreringstype = new ArrayList<>();
    public List<UtdanningSvar> utdanning = new ArrayList<>();
    public List<UtdanningBestattSvar> utdanningBestatt = new ArrayList<>();
    public List<UtdanningGodkjentSvar> utdanningGodkjent = new ArrayList<>();
    public List<String> arbeidslisteKategori = new ArrayList<>();
    public List<String> sisteEndringKategori = new ArrayList<>();
    public List<String> aktiviteterForenklet = new ArrayList<>();
    public String ulesteEndringer;
    public CVjobbprofil cvJobbprofil;

    public boolean harAktiveFilter() {
        return harFerdigFilter() ||
                harYtelsefilter() ||
                !alder.isEmpty() ||
                harKjonnfilter() ||
                !fodselsdagIMnd.isEmpty() ||
                !innsatsgruppe.isEmpty() ||
                !formidlingsgruppe.isEmpty() ||
                !servicegruppe.isEmpty() ||
                !rettighetsgruppe.isEmpty() ||
                !veiledere.isEmpty() ||
                !aktiviteter.isEmpty() ||
                !tiltakstyper.isEmpty() ||
                !hovedmal.isEmpty() ||
                !registreringstype.isEmpty() ||
                !utdanning.isEmpty() ||
                !utdanningBestatt.isEmpty() ||
                !utdanningGodkjent.isEmpty() ||
                !sisteEndringKategori.isEmpty() ||
                !arbeidslisteKategori.isEmpty() ||
                harAktiviteterForenklet() ||
                harCvFilter() ||
                harManuellBrukerStatus() ||
                harNavnEllerFnrQuery();
    }

    public boolean harCvFilter() {
        return cvJobbprofil != null;
    }

    public boolean harFerdigFilter() {
        return brukerstatus != null || (ferdigfilterListe != null && !ferdigfilterListe.isEmpty());
    }

    public boolean harYtelsefilter() {
        return ytelse != null;
    }

    public boolean harKjonnfilter() {
        return kjonn != null;
    }

    public boolean harAktivitetFilter() {
        return tiltakstyper != null;
    }

    public boolean harSisteEndringFilter() {
        return !sisteEndringKategori.isEmpty();
    }

    public boolean harManuellBrukerStatus() {
        return manuellBrukerStatus != null && !manuellBrukerStatus.isEmpty();
    }

    public boolean harAktiviteterForenklet(){
        return !aktiviteterForenklet.isEmpty();
    }

    public boolean harNavnEllerFnrQuery() {
        return StringUtils.isNotBlank(navnEllerFnrQuery);
    }

    public boolean harUlesteEndringerFilter() {
        return StringUtils.isNotBlank(ulesteEndringer);
    }

    public boolean valider() {
        if (!harAktiveFilter()) {
            return true;
        }

        Boolean alderOk = alder
                .stream()
                .map(alderInput -> (FiltervalgMappers.alder.containsKey(alderInput) || FiltervalgMappers.isValidDynamicRange(alderInput)))
                .reduce(true, and());

        Boolean fodselsdatoOk = fodselsdagIMnd
                .stream()
                .map((dato) -> dato.matches("\\d+"))
                .reduce(true, and());
        Boolean veiledereOk = veiledere
                .stream()
                .map((veileder) -> veileder.matches("[A-Z]\\d{6}"))
                .reduce(true, and());
        Boolean utdanningOK = utdanning
                .stream()
                .map(Objects::nonNull)
                .reduce(true, and());

        Boolean sisteEndringOK = sisteEndringKategori
                .stream()
                .map(SisteEndringsKategori::contains)
                .reduce(true, and());

        return alderOk && fodselsdatoOk && veiledereOk && utdanningOK && sisteEndringOK;
    }

    private BinaryOperator<Boolean> and() {
        return (aBoolean, aBoolean2) -> aBoolean && aBoolean2;
    }
}
