package no.nav.pto.veilarbportefolje.postgres;

import lombok.SneakyThrows;
import net.logstash.logback.encoder.org.apache.commons.lang3.StringUtils;
import no.nav.pto.veilarbportefolje.database.PostgresTable;
import no.nav.pto.veilarbportefolje.database.Table;
import no.nav.pto.veilarbportefolje.domene.*;
import no.nav.pto.veilarbportefolje.postgres.sort.PostgresSortQueryBuilder;
import no.nav.pto.veilarbportefolje.postgres.sort.SortOrder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Integer.parseInt;
import static java.util.stream.Collectors.toList;
import static no.nav.pto.veilarbportefolje.database.PostgresTable.BRUKER_VIEW.*;
import static no.nav.pto.veilarbportefolje.domene.AktivitetFiltervalg.JA;
import static no.nav.pto.veilarbportefolje.domene.AktivitetFiltervalg.NEI;
import static no.nav.pto.veilarbportefolje.postgres.Utils.contains;
import static no.nav.pto.veilarbportefolje.postgres.Utils.eq;

public class PostgresQueryBuilder {
    private final String tableAlias = "tbl_bruker";
    private final StringJoiner columns = new StringJoiner(", ");
    private String mainTable = "";
    private final StringJoiner joinedTables = new StringJoiner(",", ", ", "");
    private final StringJoiner whereStatement = new StringJoiner(" AND ");
    private final PostgresSortQueryBuilder postgresSortQueryBuilder;
    private final JdbcTemplate db;
    private boolean vedtaksPilot;
    private boolean brukKunEssensiellInfo = true;

    public PostgresQueryBuilder(@Qualifier("PostgresJdbc") JdbcTemplate jdbcTemplate, String navKontor, boolean vedtaksPilot) {
        this.db = jdbcTemplate;
        this.vedtaksPilot = vedtaksPilot;
        columns.add(tableAlias + ".*");
        whereStatement.add(eq(NAV_KONTOR, navKontor));
        whereStatement.add(eq(OPPFOLGING, true));
        postgresSortQueryBuilder = new PostgresSortQueryBuilder();
    }

    public BrukereMedAntall search(Integer fra, Integer antall) {
        List<Map<String, Object>> resultat;

        if (brukKunEssensiellInfo) {
            mainTable = PostgresTable.OPTIMALISER_BRUKER_VIEW.TABLE_NAME + " " + tableAlias;
        } else {
            mainTable = PostgresTable.BRUKER_VIEW.TABLE_NAME + " " + tableAlias;
        }

        resultat = db.queryForList(getAssembledSearchQuery());

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

    public void veiledereFilter(List<String> veiledere) {
        whereStatement.add(contains(VEILEDERID, veiledere));
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

    public void iavtaltAktivitet() {
        joinedTables.add("LATERAL (SELECT " + PostgresTable.AKTIVITETTYPE_STATUS.AKTOERID + ", MIN(" + PostgresTable.AKTIVITETTYPE_STATUS.NESTE_UTLOPSDATO + ") as NESTE_UTLOPSDATO FROM " + PostgresTable.AKTIVITETTYPE_STATUS.TABLE_NAME + " atb WHERE atb.AKTOERID = " + getKeyColumn() + " GROUP BY AKTOERID) as iavt_akt");
        columns.add("iavt_akt.NESTE_UTLOPSDATO");
    }

    public void tiltaksTyperFilter(List<String> tiltakstyper) {
        String tiltakTyperSql = tiltakstyper.stream().map(x -> "'" + x + "'").collect(Collectors.joining(","));
        joinedTables.add("LATERAL (SELECT " + PostgresTable.BRUKERTILTAK.AKTOERID + " FROM " + PostgresTable.BRUKERTILTAK.TABLE_NAME + " brt WHERE brt.AKTOERID = " + getKeyColumn() + " AND " + PostgresTable.BRUKERTILTAK.TILTAKSKODE + " IN (" + tiltakTyperSql + ") GROUP BY AKTOERID) as bruk_tilt");
    }

    public void aktiviteterForenkletFilter(List<String> aktiviteterForenklet) {
        String aktiviteterSql = aktiviteterForenklet.stream().map(x -> "'" + x + "'").collect(Collectors.joining(","));
        joinedTables.add("LATERAL (SELECT " + PostgresTable.AKTIVITETTYPE_STATUS.AKTOERID + ", MIN(" + PostgresTable.AKTIVITETTYPE_STATUS.NESTE_UTLOPSDATO + ") as NESTE_UTLOPSDATO FROM " + PostgresTable.AKTIVITETTYPE_STATUS.TABLE_NAME + " atb WHERE atb.AKTOERID = " + getKeyColumn() + " AND " + PostgresTable.AKTIVITETTYPE_STATUS.AKTIVITETTYPE + " IN (" + aktiviteterSql + ") GROUP BY AKTOERID) as aktivt");
        columns.add("aktivt.NESTE_UTLOPSDATO");
    }

    public void aktivitetFilter(Map<String, AktivitetFiltervalg> aktiviteter) {
        List<String> includeAktiviteter = new ArrayList<>();
        List<String> excludeAktiviteter = new ArrayList<>();
        String aktiviteterSql = "";
        aktiviteter.forEach((key, value) -> {
            if (value.equals(JA)) {
                includeAktiviteter.add(key);
            } else if (value.equals(NEI)) {
                excludeAktiviteter.add(key);
            }
        });
        if (includeAktiviteter.isEmpty() && excludeAktiviteter.isEmpty()) {
            return;
        }
        if (!includeAktiviteter.isEmpty()) {
            aktiviteterSql += " AND " + PostgresTable.AKTIVITETTYPE_STATUS.AKTIVITETTYPE + " IN (" + includeAktiviteter.stream().map(x -> "'" + x + "'").collect(Collectors.joining(",")) + ")";
        }
        if (!excludeAktiviteter.isEmpty()) {
            aktiviteterSql += " AND " + PostgresTable.AKTIVITETTYPE_STATUS.AKTIVITETTYPE + " NOT IN (" + excludeAktiviteter.stream().map(x -> "'" + x + "'").collect(Collectors.joining(",")) + ")";
        }
        joinedTables.add("LATERAL (SELECT " + PostgresTable.AKTIVITETTYPE_STATUS.AKTOERID + ", MIN(" + PostgresTable.AKTIVITETTYPE_STATUS.NESTE_UTLOPSDATO + ") as NESTE_UTLOPSDATO FROM " + PostgresTable.AKTIVITETTYPE_STATUS.TABLE_NAME + " atb WHERE atb.AKTOERID = " + getKeyColumn() + " " + aktiviteterSql + " GROUP BY AKTOERID) as aktivt");
        columns.add("aktivt.NESTE_UTLOPSDATO");
    }

    /*
     * TODO: find out from which table we should read data
     * */
    public void ytelserFilter(List<YtelseMapping> underytelser) {
        String ytelserSql = underytelser.stream().map(x -> "'" + x.name() + "'").collect(Collectors.joining(","));
    }

    public void ulesteEndringerFilter() {
        joinedTables.add("LATERAL (SELECT " + Table.SISTE_ENDRING.AKTOERID + " FROM " + Table.SISTE_ENDRING.TABLE_NAME + " sist WHERE sist.AKTOERID = " + getKeyColumn() + " AND " + Table.SISTE_ENDRING.ER_SETT + " GROUP BY AKTOERID) as sist_endr");
    }

    public void sisteEndringFilter(List<String> sisteEndringKategori) {
        String sisteEndringSql = sisteEndringKategori.stream().map(x -> "'" + x + "'").collect(Collectors.joining(","));
        joinedTables.add("LATERAL (SELECT " + Table.SISTE_ENDRING.AKTOERID + ", MIN(siste_endring_tidspunkt) AS siste_endring_tidspunkt FROM " + Table.SISTE_ENDRING.TABLE_NAME + " sist WHERE sist.AKTOERID = " + getKeyColumn() + " AND " + Table.SISTE_ENDRING.SISTE_ENDRING_KATEGORI + " IN (" + sisteEndringSql + ") GROUP BY AKTOERID) as sist_endr");
        columns.add("sist_endr.siste_endring_tidspunkt");
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

    private String getAssembledSearchQuery() {
        StringJoiner computedSqlQuery = new StringJoiner(" ");

        computedSqlQuery.add("SELECT");
        computedSqlQuery.add(columns.toString());
        computedSqlQuery.add("FROM");
        computedSqlQuery.add(mainTable);
        computedSqlQuery.add(joinedTables.toString());
        computedSqlQuery.add("WHERE");
        computedSqlQuery.add(whereStatement.toString());
        computedSqlQuery.add("ORDER BY");
        computedSqlQuery.add(getSortStatement());
        return computedSqlQuery.toString();
    }

    private String getKeyColumn() {
        return tableAlias + "." + AKTOERID;
    }

    private String getSortStatement() {
        return postgresSortQueryBuilder.getSortStatement();
    }
}
