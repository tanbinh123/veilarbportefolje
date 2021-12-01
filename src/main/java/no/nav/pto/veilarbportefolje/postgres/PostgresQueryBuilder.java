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
    private final List<String> joinedTables = new ArrayList<>();
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
        postgresSortQueryBuilder.createSortStatements(sortField, order, kallesFraMinOversikt);
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

    public void veiledereFilter(List<String> veiledere) {
        whereStatement.add(contains(VEILEDERID, veiledere));
    }

    public void ufordeltBruker(List<String> veiledereMedTilgangTilEnhet) {
        String veiledere = veiledereMedTilgangTilEnhet.stream().map(Object::toString).collect(Collectors.joining(",", "{", "}"));
        whereStatement.add("(" + VEILEDERID + " IS NULL OR " + VEILEDERID + " <> ALL ('" + veiledere + "'::varchar[]))");
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
        String tableAlias = "iavt_akt";
        joinedTables.add("LATERAL (SELECT " + PostgresTable.AKTIVITETTYPE_STATUS.AKTOERID + ", MIN(" + PostgresTable.AKTIVITETTYPE_STATUS.NESTE_UTLOPSDATO + ") as neste_utlopsdato, json_agg(" + PostgresTable.AKTIVITETTYPE_STATUS.AKTIVITETTYPE + ") AS aktiviteter_array, json_agg(" + PostgresTable.AKTIVITETTYPE_STATUS.NESTE_UTLOPSDATO + ") as neste_utlopsdato_array FROM " + PostgresTable.AKTIVITETTYPE_STATUS.TABLE_NAME + " atb WHERE atb.AKTOERID = " + getKeyColumn() + " AND aktiv GROUP BY AKTOERID) as " + tableAlias);
        columns.add(tableAlias + ".neste_utlopsdato");
        columns.add(tableAlias + ".aktiviteter_array");
        columns.add(tableAlias + ".neste_utlopsdato_array");
    }

    public void ikkeIAvtaltAktivitet() {
        whereStatement.add(getKeyColumn() + " NOT IN (SELECT " + PostgresTable.AKTIVITETTYPE_STATUS.AKTOERID + " FROM " + PostgresTable.AKTIVITETTYPE_STATUS.TABLE_NAME + " WHERE aktiv)");
    }

    public void utlopteAktivitet() {
        String tableAlias = "utlp_akt";
        joinedTables.add("LATERAL (SELECT " + PostgresTable.AKTIVITET_STATUS.AKTOERID + ", MAX(" + PostgresTable.AKTIVITET_STATUS.NYESTEUTLOPTEAKTIVITET + ") as NYESTEUTLOPTEAKTIVITET FROM " + PostgresTable.AKTIVITET_STATUS.TABLE_NAME + " atb WHERE atb.AKTOERID = " + getKeyColumn() + " AND " + PostgresTable.AKTIVITET_STATUS.NYESTEUTLOPTEAKTIVITET + " IS NOT NULL GROUP BY AKTOERID) as " + tableAlias);
        columns.add(tableAlias + ".NYESTEUTLOPTEAKTIVITET");
    }

    public void tiltaksTyperFilter(List<String> tiltakstyper) {
        String tiltakTyperSql = tiltakstyper.stream().map(x -> "'" + x + "'").collect(Collectors.joining(","));
        joinedTables.add("LATERAL (SELECT " + PostgresTable.BRUKERTILTAK.AKTOERID + " FROM " + PostgresTable.BRUKERTILTAK.TABLE_NAME + " brt WHERE brt.AKTOERID = " + getKeyColumn() + " AND " + PostgresTable.BRUKERTILTAK.TILTAKSKODE + " IN (" + tiltakTyperSql + ") GROUP BY AKTOERID) as bruk_tilt");
    }

    public void aktiviteterForenkletFilter(List<String> aktiviteterForenklet) {
        String tableAlias = "aktivt";
        String aktiviteterSql = aktiviteterForenklet.stream().map(x -> "'" + x.toLowerCase() + "'").collect(Collectors.joining(","));
        joinedTables.add("LATERAL (SELECT " + PostgresTable.AKTIVITETTYPE_STATUS.AKTOERID + ", MIN(" + PostgresTable.AKTIVITETTYPE_STATUS.NESTE_UTLOPSDATO + ") as NESTE_UTLOPSDATO FROM " + PostgresTable.AKTIVITETTYPE_STATUS.TABLE_NAME + " atb WHERE atb.AKTOERID = " + getKeyColumn() + " AND " + PostgresTable.AKTIVITETTYPE_STATUS.AKTIVITETTYPE + " IN (" + aktiviteterSql + ") AND aktiv GROUP BY AKTOERID) as " + tableAlias);
        columns.add(tableAlias + ".NESTE_UTLOPSDATO");
    }

    public void aktivitetFilter(Map<String, AktivitetFiltervalg> aktiviteter) {
        String tableAlias = "aktivt";
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
        joinedTables.add("LATERAL (SELECT " + PostgresTable.AKTIVITETTYPE_STATUS.AKTOERID + ", MIN(" + PostgresTable.AKTIVITETTYPE_STATUS.NESTE_UTLOPSDATO + ") as NESTE_UTLOPSDATO FROM " + PostgresTable.AKTIVITETTYPE_STATUS.TABLE_NAME + " atb WHERE atb.AKTOERID = " + getKeyColumn() + " " + aktiviteterSql + " AND aktiv GROUP BY AKTOERID) as " + tableAlias);
        columns.add(tableAlias + ".NESTE_UTLOPSDATO");
    }

    public void ytelserFilter(List<YtelseMapping> underytelser) {
        String tableAlias = "ytls";
        String ytelserSql = underytelser.stream().map(x -> "'" + x.name() + "'").collect(Collectors.joining(","));
        joinedTables.add("LATERAL (SELECT " + PostgresTable.YTELSE_STATUS_FOR_BRUKER.AKTOERID + ",  MIN(" + PostgresTable.YTELSE_STATUS_FOR_BRUKER.DAGPUTLOPUKE + ")  AS aapmaxtiduke, MAX(aapmaxtiduke, aapunntakdagerigjen / 5) AS aaprettighetsperiode, MIN(" + PostgresTable.YTELSE_STATUS_FOR_BRUKER.AAPUNNTAKDAGERIGJEN + " / 5) as aapunntakukerigjen, MIN(" + PostgresTable.YTELSE_STATUS_FOR_BRUKER.DAGPUTLOPUKE + ") as dagputlopuke FROM " + PostgresTable.YTELSE_STATUS_FOR_BRUKER.TABLE_NAME + " ytls WHERE ytls.AKTOERID = " + getKeyColumn() + " AND " + PostgresTable.YTELSE_STATUS_FOR_BRUKER.YTELSE + " IN (" + ytelserSql + ") GROUP BY AKTOERID) as " + tableAlias);
        columns.add(tableAlias + "." + PostgresTable.YTELSE_STATUS_FOR_BRUKER.DAGPUTLOPUKE);
        columns.add(tableAlias + ".aapmaxtiduke");
        columns.add(tableAlias + ".aaprettighetsperiode");
        columns.add(tableAlias + ".aapunntakukerigjen");
        columns.add(tableAlias + ".dagputlopuke");
    }

    public void ulesteEndringerFilter() {
        joinedTables.add("LATERAL (SELECT " + Table.SISTE_ENDRING.AKTOERID + " FROM " + Table.SISTE_ENDRING.TABLE_NAME + " sist WHERE sist.AKTOERID = " + getKeyColumn() + " AND " + Table.SISTE_ENDRING.ER_SETT + " GROUP BY AKTOERID) as sist_endr");
    }

    public void sisteEndringFilter(List<String> sisteEndringKategori) {
        String tableAlias = "sist_endr";
        String sisteEndringSql = sisteEndringKategori.stream().map(x -> "'" + x + "'").collect(Collectors.joining(","));
        joinedTables.add("LATERAL (SELECT " + Table.SISTE_ENDRING.AKTOERID + ", MIN(siste_endring_tidspunkt) AS siste_endring_tidspunkt FROM " + Table.SISTE_ENDRING.TABLE_NAME + " sist WHERE sist.AKTOERID = " + getKeyColumn() + " AND " + Table.SISTE_ENDRING.SISTE_ENDRING_KATEGORI + " IN (" + sisteEndringSql + ") GROUP BY AKTOERID) as sist_endr");
        columns.add(tableAlias + ".siste_endring_tidspunkt");
    }

    public void moterIDag() {
        String tableAlias = "moter_idag";
        joinedTables.add("LATERAL (SELECT " + PostgresTable.AKTIVITETTYPE_STATUS.AKTOERID + ", MIN(" + PostgresTable.AKTIVITETTYPE_STATUS.NESTE_STARTDATO + ") as neste_startdato, min(" + PostgresTable.AKTIVITETTYPE_STATUS.NESTE_UTLOPSDATO + "::timestamp) - min(" + PostgresTable.AKTIVITETTYPE_STATUS.NESTE_STARTDATO + "::timestamp) as duration FROM " + PostgresTable.AKTIVITETTYPE_STATUS.TABLE_NAME + " mott WHERE mott.AKTOERID  = " + getKeyColumn() + " AND aktiv AND aktivitettype = 'mote' AND neste_startdato::date = current_timestamp::date GROUP BY AKTOERID) as " + tableAlias);
        columns.add(tableAlias + ".neste_startdato");
        columns.add(tableAlias + ".duration");
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
        computedSqlQuery.add(joinedTables.stream().map(x -> ", " + x).collect(Collectors.joining()));
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
