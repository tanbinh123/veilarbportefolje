package no.nav.pto.veilarbportefolje.database;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Try;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import no.nav.common.types.identer.AktorId;
import no.nav.common.types.identer.Fnr;
import no.nav.pto.veilarbportefolje.database.Table.OPPFOLGINGSBRUKER;
import no.nav.pto.veilarbportefolje.database.Table.OPPFOLGING_DATA;
import no.nav.pto.veilarbportefolje.database.Table.VW_PORTEFOLJE_INFO;
import no.nav.pto.veilarbportefolje.domene.AAPMaxtidUkeFasettMapping;
import no.nav.pto.veilarbportefolje.domene.AAPUnntakUkerIgjenFasettMapping;
import no.nav.pto.veilarbportefolje.domene.Brukerdata;
import no.nav.pto.veilarbportefolje.domene.DagpengerUkeFasettMapping;
import no.nav.pto.veilarbportefolje.domene.ManedFasettMapping;
import no.nav.pto.veilarbportefolje.domene.YtelseMapping;
import no.nav.pto.veilarbportefolje.domene.value.PersonId;
import no.nav.pto.veilarbportefolje.domene.value.VeilederId;
import no.nav.pto.veilarbportefolje.opensearch.domene.OppfolgingsBruker;
import no.nav.sbl.sql.SqlUtils;
import no.nav.sbl.sql.where.WhereClause;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static no.nav.pto.veilarbportefolje.database.Table.AKTOERID_TO_PERSONID;
import static no.nav.pto.veilarbportefolje.database.Table.VW_PORTEFOLJE_INFO.AKTOERID;
import static no.nav.pto.veilarbportefolje.database.Table.VW_PORTEFOLJE_INFO.FODSELSNR;
import static no.nav.pto.veilarbportefolje.util.DbUtils.mapTilOppfolgingsBruker;
import static no.nav.pto.veilarbportefolje.util.DbUtils.not;
import static no.nav.pto.veilarbportefolje.util.DbUtils.parseJaNei;
import static no.nav.pto.veilarbportefolje.util.StreamUtils.batchProcess;
import static no.nav.sbl.sql.SqlUtils.insert;
import static no.nav.sbl.sql.SqlUtils.select;
import static no.nav.sbl.sql.SqlUtils.update;
import static no.nav.sbl.sql.SqlUtils.upsert;
import static no.nav.sbl.sql.where.WhereClause.in;

@Slf4j
@Repository
public class BrukerRepository {

    private final JdbcTemplate db;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    @Autowired
    public BrukerRepository(JdbcTemplate db, NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
        this.db = db;
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
    }

    public Optional<Integer> hentAntallBrukereUnderOppfolging() {
        Integer count = db.query(countOppfolgingsBrukereSql(), rs -> {
            rs.next();
            return rs.getInt(1);
        });
        return ofNullable(count);
    }

    private String countOppfolgingsBrukereSql() {
        return "SELECT COUNT(*) FROM VW_PORTEFOLJE_INFO " +
                "WHERE FORMIDLINGSGRUPPEKODE = 'ARBS' " +
                "OR OPPFOLGING = 'J' " +
                "OR (FORMIDLINGSGRUPPEKODE = 'IARBS' AND KVALIFISERINGSGRUPPEKODE IN ('BATT', 'BFORM', 'VARIG', 'IKVAL', 'VURDU', 'OPPFI'))";
    }

    public Optional<AktorId> hentAktorIdFraView(Fnr fnr) {
        return Optional.ofNullable(
                select(db, VW_PORTEFOLJE_INFO.TABLE_NAME, rs -> rs.getString("AKTOERID"))
                        .column("AKTOERID")
                        .where(WhereClause.equals("FODSELSNR", fnr.toString()))
                        .execute()
        ).map(AktorId::of);
    }

    public Optional<AktorId> hentAktorIdFraView(PersonId personid) {
        return Optional.ofNullable(
                select(db, VW_PORTEFOLJE_INFO.TABLE_NAME, rs -> rs.getString("AKTOERID"))
                        .column("AKTOERID")
                        .where(WhereClause.equals("PERSON_ID", personid.toString()))
                        .execute()
        ).map(AktorId::of);
    }

    public Optional<OppfolgingsBruker> hentBrukerFraView(AktorId aktoerId) {
        final OppfolgingsBruker bruker = select(db, VW_PORTEFOLJE_INFO.TABLE_NAME, rs -> mapTilOppfolgingsBruker(rs))
                .column("*")
                .where(WhereClause.equals("AKTOERID", aktoerId.toString()))
                .execute();

        return Optional.ofNullable(bruker);
    }


    public List<OppfolgingsBruker> hentBrukereFraView(List<AktorId> aktorIds) {
        db.setFetchSize(1000);
        List<String> ids = aktorIds.stream().map(AktorId::get).collect(toList());
        return SqlUtils
                .select(db, VW_PORTEFOLJE_INFO.TABLE_NAME, rs -> erUnderOppfolging(rs) ? mapTilOppfolgingsBruker(rs) : null)
                .column("*")
                .where(in("AKTOERID", ids))
                .executeToList()
                .stream()
                .filter(Objects::nonNull)
                .toList();
    }

    @SneakyThrows
    public static boolean erUnderOppfolging(ResultSet rs) {
        return parseJaNei(rs.getString("OPPFOLGING"), "OPPFOLGING");
    }

    public Optional<VeilederId> hentVeilederForBruker(AktorId aktoerId) {
        VeilederId veilederId = select(db, OPPFOLGING_DATA.TABLE_NAME, this::mapToVeilederId)
                .column(OPPFOLGING_DATA.VEILEDERIDENT)
                .where(WhereClause.equals(OPPFOLGING_DATA.AKTOERID, aktoerId.toString()))
                .execute();

        return Optional.ofNullable(veilederId);
    }


    @Deprecated
    public Try<VeilederId> retrieveVeileder(AktorId aktoerId) {
        return Try.of(
                () -> select(db, "OPPFOLGING_DATA", this::mapToVeilederId)
                        .column("VEILEDERIDENT")
                        .where(WhereClause.equals("AKTOERID", aktoerId.toString()))
                        .execute()
        ).onFailure(e -> log.warn("Fant ikke veileder for bruker med aktoerId {}", aktoerId));
    }

    public Optional<String> hentNavKontorFraView(AktorId aktoerId) {
        String navKontor = select(db, VW_PORTEFOLJE_INFO.TABLE_NAME, this::mapToEnhet)
                .column(VW_PORTEFOLJE_INFO.NAV_KONTOR)
                .where(WhereClause.equals(AKTOERID, aktoerId.toString()))
                .execute();

        return Optional.ofNullable(navKontor);
    }

    public Optional<String> hentNavKontorFraDbLinkTilArena(Fnr fnr) {
        String navKontor = select(db, OPPFOLGINGSBRUKER.TABLE_NAME, this::mapToEnhet)
                .column(OPPFOLGINGSBRUKER.NAV_KONTOR)
                .where(WhereClause.equals(FODSELSNR, fnr.toString()))
                .execute();

        return Optional.ofNullable(navKontor);
    }

    @Deprecated
    public Try<String> retrieveEnhet(Fnr fnr) {
        return Try.of(
                () -> select(db, "OPPFOLGINGSBRUKER", this::mapToEnhet)
                        .column("NAV_KONTOR")
                        .where(WhereClause.equals("FODSELSNR", fnr.toString()))
                        .execute()
        ).onFailure(e -> log.warn("Fant ikke oppfølgingsenhet for bruker"));
    }

    public void upsertAktoeridToPersonidMapping(AktorId aktoerId, PersonId personId) {
        upsert(db, AKTOERID_TO_PERSONID.TABLE_NAME)
                .set("AKTOERID", aktoerId.toString())
                .set("PERSONID", personId.toString())
                .set("GJELDENE", 1)
                .where(WhereClause.equals("AKTOERID", aktoerId.toString()))
                .execute();

    }

    public Integer insertGamleAktorIdMedGjeldeneFlaggNull(AktorId aktoerId, PersonId personId) {
        return insert(db, AKTOERID_TO_PERSONID.TABLE_NAME)
                .value("AKTOERID", aktoerId.toString())
                .value("PERSONID", personId.toString())
                .value("GJELDENE", 0)
                .execute();
    }

    public Integer setGjeldeneFlaggTilNull(PersonId personId) {
        return update(db, AKTOERID_TO_PERSONID.TABLE_NAME)
                .set("GJELDENE", 0)
                .whereEquals("PERSONID", personId.toString())
                .execute();
    }

    public Optional<List<AktorId>> hentGamleAktorIder(PersonId personId) {
        return Optional.ofNullable(SqlUtils
                .select(db, AKTOERID_TO_PERSONID.TABLE_NAME, rs -> rs == null ? null : AktorId.of(rs.getString(AKTOERID_TO_PERSONID.AKTOERID)))
                .column(AKTOERID_TO_PERSONID.AKTOERID)
                .where(
                        WhereClause.equals(AKTOERID_TO_PERSONID.PERSONID, personId.getValue())
                                .and(
                                        WhereClause.equals(AKTOERID_TO_PERSONID.GJELDENE, 0))
                ).executeToList());
    }

    public Try<PersonId> retrievePersonid(AktorId aktoerId) {
        return Try.of(
                        () -> select(db, AKTOERID_TO_PERSONID.TABLE_NAME, this::mapToPersonIdFromAktorIdToPersonId)
                                .column("PERSONID")
                                .where(WhereClause.equals("AKTOERID", aktoerId.toString()))
                                .execute()
                )
                .onFailure(e -> log.warn("Fant ikke personid for aktoerid: " + aktoerId, e));
    }

    public Optional<PersonId> retrievePersonidFromFnr(Fnr fnr) {
        Optional<PersonId> personId = ofNullable(
                select(db, "OPPFOLGINGSBRUKER", this::mapPersonIdFromOppfolgingsbruker)
                        .column("PERSON_ID")
                        .where(WhereClause.equals("FODSELSNR", fnr.toString()))
                        .execute()
        );
        if (personId.isEmpty()) {
            log.warn("Fant ikke personid for fnr: " + fnr);
        }
        return personId;
    }

    public Try<Fnr> retrieveFnrFromPersonid(PersonId personId) {
        return Try.of(() ->
                select(db, "OPPFOLGINGSBRUKER", this::mapFnrFromOppfolgingsbruker)
                        .column("FODSELSNR")
                        .where(WhereClause.equals("PERSON_ID", personId.toString()))
                        .execute()
        ).onFailure(e -> log.warn("Fant ikke fnr for personid: " + personId, e));
    }

    /**
     * MAPPING-FUNKSJONER
     */
    @SneakyThrows
    private String mapToEnhet(ResultSet rs) {
        return rs.getString("NAV_KONTOR");
    }

    @SneakyThrows
    private VeilederId mapToVeilederId(ResultSet rs) {
        return rs.getString("VEILEDERIDENT") == null ? null : VeilederId.of(rs.getString("VEILEDERIDENT"));
    }

    @SneakyThrows
    private PersonId mapToPersonIdFromAktorIdToPersonId(ResultSet rs) {
        return PersonId.of(rs.getString("PERSONID"));
    }

    @SneakyThrows
    private PersonId mapPersonIdFromOppfolgingsbruker(ResultSet resultSet) {
        return PersonId.of(Integer.toString(resultSet.getBigDecimal("PERSON_ID").intValue()));
    }

    @SneakyThrows
    private Fnr mapFnrFromOppfolgingsbruker(ResultSet resultSet) {
        return Fnr.ofValidFnr(resultSet.getString("FODSELSNR"));
    }

    public List<Brukerdata> retrieveBrukerdata(List<String> personIds) {
        Map<String, Object> params = new HashMap<>();
        params.put("fnrs", personIds);
        String sql = retrieveBrukerdataSQL();
        return namedParameterJdbcTemplate.queryForList(sql, params)
                .stream()
                .map(data -> new Brukerdata()
                        .setAktoerid((String) data.get("AKTOERID"))
                        .setPersonid((String) data.get("PERSONID"))
                        .setYtelse(ytelsemappingOrNull((String) data.get("YTELSE")))
                        .setUtlopsdato(toLocalDateTime((Timestamp) data.get("UTLOPSDATO")))
                        .setUtlopsFasett(manedmappingOrNull((String) data.get("UTLOPSDATOFASETT")))
                        .setDagputlopUke(intValue(data.get("DAGPUTLOPUKE")))
                        .setDagputlopUkeFasett(dagpengerUkeFasettMappingOrNull((String) data.get("DAGPUTLOPUKEFASETT")))
                        .setPermutlopUke(intValue(data.get("PERMUTLOPUKE")))
                        .setPermutlopUkeFasett(dagpengerUkeFasettMappingOrNull((String) data.get("PERMUTLOPUKEFASETT")))
                        .setAapmaxtidUke(intValue(data.get("AAPMAXTIDUKE")))
                        .setAapmaxtidUkeFasett(aapMaxtidUkeFasettMappingOrNull((String) data.get("AAPMAXTIDUKEFASETT")))
                        .setAapUnntakDagerIgjen(intValue(data.get("AAPUNNTAKDAGERIGJEN")))
                        .setAapunntakUkerIgjenFasett(aapUnntakUkerIgjenFasettMappingOrNull((String) data.get("AAPUNNTAKUKERIGJENFASETT")))
                        .setNyesteUtlopteAktivitet((Timestamp) data.get("NYESTEUTLOPTEAKTIVITET"))
                        .setAktivitetStart((Timestamp) data.get("AKTIVITET_START"))
                        .setNesteAktivitetStart((Timestamp) data.get("NESTE_AKTIVITET_START"))
                        .setForrigeAktivitetStart((Timestamp) data.get("FORRIGE_AKTIVITET_START")))
                .collect(toList());
    }

    public Map<String, Optional<String>> retrievePersonidFromFnrs(Collection<String> fnrs) {
        Map<String, Optional<String>> brukere = new HashMap<>(fnrs.size());

        batchProcess(1000, fnrs, (fnrBatch) -> {
            Map<String, Object> params = new HashMap<>();
            params.put("fnrs", fnrBatch);
            String sql = getPersonIdsFromFnrsSQL();
            Map<String, Optional<String>> fnrPersonIdMap = namedParameterJdbcTemplate.queryForList(sql, params)
                    .stream()
                    .map((rs) -> Tuple.of(
                            (String) rs.get("FODSELSNR"),
                            rs.get("PERSON_ID").toString())
                    )
                    .collect(Collectors.toMap(Tuple2::_1, personData -> Optional.of(personData._2())));

            brukere.putAll(fnrPersonIdMap);
        });

        fnrs.stream()
                .filter(not(brukere::containsKey))
                .forEach((ikkeFunnetBruker) -> brukere.put(ikkeFunnetBruker, empty()));

        return brukere;
    }

    public List<PersonId> hentMappedePersonIder(AktorId aktorId) {
        final String sql = "SELECT PERSONID FROM AKTOERID_TO_PERSONID WHERE GJELDENE = 1 AND AKTOERID = ?";
        return db.queryForList(sql, String.class, aktorId.get())
                .stream()
                .map(PersonId::of)
                .toList();
    }

    public void insertOrUpdateBrukerdata(List<Brukerdata> brukerdata, Collection<String> finnesIDb) {
        Map<Boolean, List<Brukerdata>> eksisterendeBrukere = brukerdata
                .stream()
                .collect(groupingBy((data) -> finnesIDb.contains(data.getPersonid())));

        Brukerdata.batchUpdate(db, eksisterendeBrukere.getOrDefault(true, emptyList()));

        eksisterendeBrukere
                .getOrDefault(false, emptyList())
                .forEach(this::upsertBrukerdata);
    }

    void upsertBrukerdata(Brukerdata brukerdata) {
        brukerdata.toUpsertQuery(db).execute();
    }

    String retrieveSistIndeksertSQL() {
        return "SELECT SIST_INDEKSERT FROM METADATA";
    }

    String updateSistIndeksertSQL() {
        return "UPDATE METADATA SET SIST_INDEKSERT = ?";
    }

    private String getPersonIdsFromFnrsSQL() {
        return
                "SELECT " +
                        "person_id, " +
                        "fodselsnr " +
                        "FROM " +
                        "OPPFOLGINGSBRUKER " +
                        "WHERE " +
                        "fodselsnr in (:fnrs)";
    }

    private String retrieveBrukerdataSQL() {
        return "SELECT * FROM BRUKER_DATA WHERE PERSONID in (:fnrs)";
    }

    private static Integer intValue(Object value) {
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).intValue();
        } else if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        } else {
            return null;
        }
    }

    private static LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return timestamp != null ? timestamp.toLocalDateTime() : null;
    }

    private ManedFasettMapping manedmappingOrNull(String string) {
        return string != null ? ManedFasettMapping.valueOf(string) : null;
    }

    private YtelseMapping ytelsemappingOrNull(String string) {
        return string != null ? YtelseMapping.valueOf(string) : null;
    }

    private AAPMaxtidUkeFasettMapping aapMaxtidUkeFasettMappingOrNull(String string) {
        return string != null ? AAPMaxtidUkeFasettMapping.valueOf(string) : null;
    }

    private AAPUnntakUkerIgjenFasettMapping aapUnntakUkerIgjenFasettMappingOrNull(String string) {
        return string != null ? AAPUnntakUkerIgjenFasettMapping.valueOf(string) : null;
    }

    private DagpengerUkeFasettMapping dagpengerUkeFasettMappingOrNull(String string) {
        return string != null ? DagpengerUkeFasettMapping.valueOf(string) : null;
    }

}
