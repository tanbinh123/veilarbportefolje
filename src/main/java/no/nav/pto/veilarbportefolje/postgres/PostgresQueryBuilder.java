package no.nav.pto.veilarbportefolje.postgres;

import lombok.SneakyThrows;
import net.logstash.logback.encoder.org.apache.commons.lang3.StringUtils;
import no.nav.pto.veilarbportefolje.database.PostgresTable;
import no.nav.pto.veilarbportefolje.domene.Bruker;
import no.nav.pto.veilarbportefolje.domene.BrukereMedAntall;
import no.nav.pto.veilarbportefolje.domene.Filtervalg;
import no.nav.pto.veilarbportefolje.domene.Kjonn;
import no.nav.pto.veilarbportefolje.postgres.sort.PostgresSortQueryBuilder;
import no.nav.pto.veilarbportefolje.postgres.sort.SortOrder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDate;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static java.lang.Integer.parseInt;
import static java.util.stream.Collectors.toList;
import static no.nav.pto.veilarbportefolje.database.PostgresTable.BRUKER_VIEW.*;

public class PostgresQueryBuilder {
    private final StringJoiner whereStatement = new StringJoiner(" AND ", " WHERE ", "");
    private final PostgresSortQueryBuilder postgresSortQueryBuilder;
    private final JdbcTemplate db;
    private boolean vedtaksPilot;
    private boolean brukKunEssensiellInfo = true;

    public PostgresQueryBuilder(@Qualifier("PostgresJdbc") JdbcTemplate jdbcTemplate, String navKontor, boolean vedtaksPilot) {
        this.db = jdbcTemplate;
        this.vedtaksPilot = vedtaksPilot;
        postgresSortQueryBuilder = new PostgresSortQueryBuilder();
        whereStatement.add(eq(NAV_KONTOR, navKontor));
        whereStatement.add(eq(OPPFOLGING, true));

    }

    public BrukereMedAntall search(Integer fra, Integer antall) {
        List<Map<String, Object>> resultat;
        if (brukKunEssensiellInfo) {
            resultat = db.queryForList("SELECT * FROM " + PostgresTable.OPTIMALISER_BRUKER_VIEW.TABLE_NAME + whereStatement + postgresSortQueryBuilder.getSortStatement());
        } else {
            resultat = db.queryForList("SELECT * FROM " + TABLE_NAME + whereStatement + postgresSortQueryBuilder.getSortStatement());
        }

        List<Bruker> avskjertResultat;
        if (resultat.size() <= fra) {
            avskjertResultat = new LinkedList<>();
        } else {
            int tilIndex = (resultat.size() <= fra + antall) ? resultat.size() : fra + antall;
            avskjertResultat = resultat.subList(fra, tilIndex)
                    .stream()
                    .map(this::mapTilBruker)
                    .collect(toList());
        }

        return new BrukereMedAntall(resultat.size(), avskjertResultat);
    }

    public void sorterQueryParametere(String sortOrder, String sortField, Filtervalg filtervalg, boolean kallesFraMinOversikt) {
        SortOrder order = "ascending".equals(sortOrder) ? SortOrder.ASC : SortOrder.DESC;
        postgresSortQueryBuilder.createSortStatements(sortField, order, filtervalg, kallesFraMinOversikt);
    }

    public <T> void leggTilListeFilter(List<T> filtervalgsListe, String columnName) {
        if (!filtervalgsListe.isEmpty()) {
            brukKunEssensiellInfo = false;
            StringJoiner orStatement = new StringJoiner(" OR ", "(", ")");
            filtervalgsListe.forEach(filtervalg -> orStatement.add(columnName + " = '" + filtervalg + "'"));
            whereStatement.add(orStatement.toString());
        }
    }

    public void leggTilFodselsdagFilter(List<Integer> fodselsdager) {
        if (!fodselsdager.isEmpty()) {
            brukKunEssensiellInfo = false;
            StringJoiner orStatement = new StringJoiner(" OR ", "(", ")");
            fodselsdager.forEach(fodselsDag -> orStatement.add("date_part('DAY'," + FODSELS_DATO + ")" + " = " + fodselsDag));
            whereStatement.add(orStatement.toString());
        }
    }

    public void minOversiktFilter(String veilederId) {
        whereStatement.add(eq(VEILEDERID, veilederId));
    }

    public void enhetOversiktFilter(List<String> veiledereMedTilgangTilEnhet) {
        whereStatement.add(contains(VEILEDERID, veiledereMedTilgangTilEnhet));
    }

    public void ufordeltBruker(List<String> veiledereMedTilgangTilEnhet) {
        String veiledere = veiledereMedTilgangTilEnhet.stream().map(Object::toString).collect(Collectors.joining(",", "{", "}"));
        whereStatement.add("(" + VEILEDERID + " IS NULL OR " + VEILEDERID + " <> ANY ('" + veiledere + "'::varchar[]))");
    }

    public void nyForVeileder() {
        whereStatement.add(NY_FOR_VEILEDER);
    }

    public void erManuell() {
        brukKunEssensiellInfo = false;
        whereStatement.add(MANUELL);
    }

    public void ikkeServiceBehov() {
        brukKunEssensiellInfo = false;
        whereStatement.add(FORMIDLINGSGRUPPEKODE + " = 'ISERV'");
    }

    public void venterPaSvarFraBruker() {
        brukKunEssensiellInfo = false;
        whereStatement.add(VENTER_PA_BRUKER + " IS NOT NULL");
    }

    public void venterPaSvarFraNav() {
        brukKunEssensiellInfo = false;
        whereStatement.add(VENTER_PA_NAV + " IS NOT NULL");
    }

    public void trengerVurdering(boolean erVedtakstottePilotPa) {
        brukKunEssensiellInfo = false;
        whereStatement.add(FORMIDLINGSGRUPPEKODE + " != 'ISERV' AND " + KVALIFISERINGSGRUPPEKODE + " IN ('IVURD', 'BKART')");
        if (erVedtakstottePilotPa) {
            whereStatement.add(VEDTAKSTATUS + " IS NULL");
        }
    }

    public void underVurdering(boolean erVedtakstottePilotPa) {
        brukKunEssensiellInfo = false;
        if (erVedtakstottePilotPa) {
            whereStatement.add(VEDTAKSTATUS + " IS NOT NULL");
        } else {
            throw new IllegalStateException();
        }
    }

    public void erSykmeldtMedArbeidsgiver(boolean erVedtakstottePilotPa) {
        brukKunEssensiellInfo = false;
        whereStatement.add(FORMIDLINGSGRUPPEKODE + " = 'IARBS' AND " + KVALIFISERINGSGRUPPEKODE + " NOT IN ('BATT', 'BFORM', 'IKVAL', 'VURDU', 'OPPFI', 'VARIG')");
        if (erVedtakstottePilotPa) {
            whereStatement.add(VEDTAKSTATUS + " IS NULL");
        }
    }

    public void harArbeidsliste() {
        brukKunEssensiellInfo = false;
        whereStatement.add(ARB_ENDRINGSTIDSPUNKT + " IS NOT NULL"); // TODO: diskuter dette.
    }

    public void navnOgFodselsnummerSok(String soketekst) {
        if (StringUtils.isNumeric(soketekst)) {
            whereStatement.add(FODSELSNR + " LIKE '" + soketekst + "%'");
        } else {
            String soketekstUpper = soketekst.toUpperCase();
            whereStatement.add("(UPPER(" + FORNAVN + ") LIKE '%" + soketekstUpper + "%' OR UPPER(" + ETTERNAVN + ") LIKE '%" + soketekstUpper + "%')");
        }
    }

    public void kjonnfilter(Kjonn kjonn) {
        brukKunEssensiellInfo = false;
        whereStatement.add(KJONN + " = '" + kjonn.name() + "'");
    }

    public void alderFilter(List<String> aldere) {
        brukKunEssensiellInfo = false;
        StringJoiner orStatement = new StringJoiner(" OR ", "(", ")");
        aldere.forEach(alder -> alderFilter(alder, orStatement));
        whereStatement.add(orStatement.toString());
    }

    private void alderFilter(String alder, StringJoiner orStatement) {
        LocalDate today = LocalDate.now();
        String[] fraTilAlder = alder.split("-");
        int fraAlder = parseInt(fraTilAlder[0]);
        int tilAlder = parseInt(fraTilAlder[1]);

        LocalDate nyesteFodselsdag = today.minusYears(fraAlder);
        LocalDate eldsteFodselsDag = today.minusYears(tilAlder + 1).plusDays(1);
        orStatement.add("(" + FODSELS_DATO + " >= '" + eldsteFodselsDag.toString() + "'::date AND " + FODSELS_DATO + " <= '" + nyesteFodselsdag.toString() + "'::date" + ")");
    }

    public void harDeltCvFilter() {
        brukKunEssensiellInfo = false;
        whereStatement.add(HAR_DELT_CV);
        whereStatement.add(CV_EKSISTERER);
    }

    public void harIkkeDeltCvFilter() {
        brukKunEssensiellInfo = false;
        whereStatement.add("NOT (" + HAR_DELT_CV + " AND " + CV_EKSISTERER + ")");
    }

    @SneakyThrows
    private Bruker mapTilBruker(Map<String, Object> row) {
        Bruker bruker = new Bruker();
        if (brukKunEssensiellInfo) {
            return bruker.fraEssensiellInfo(row);
        } else {
            return bruker.fraBrukerView(row, vedtaksPilot);
        }
    }

    private String eq(String kolonne, boolean verdi) {
        if (verdi) {
            return kolonne;
        } else {
            return "NOT "+ kolonne;
        }
    }

    private String eq(String kolonne, String verdi) {
        return kolonne + " = '" + verdi + "'";
    }

    private String eq(String kolonne, int verdi) {
        return kolonne + " = " + verdi;
    }

    private String contains(String kolonne, List<String> verdier) {
        return kolonne + " IN " + verdier.stream().collect(Collectors.joining(",", "(", ")"));
    }


}
