package no.nav.pto.veilarbportefolje.arenapakafka.aktiviteter;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import no.nav.common.types.identer.AktorId;
import no.nav.common.types.identer.EnhetId;
import no.nav.pto.veilarbportefolje.aktiviteter.AktivitetDAO;
import no.nav.pto.veilarbportefolje.aktiviteter.AktivitetStatus;
import no.nav.pto.veilarbportefolje.aktiviteter.AktivitetTyper;
import no.nav.pto.veilarbportefolje.arenapakafka.arenaDTO.BrukertiltakV2;
import no.nav.pto.veilarbportefolje.arenapakafka.arenaDTO.TiltakInnhold;
import no.nav.pto.veilarbportefolje.database.Table;
import no.nav.pto.veilarbportefolje.database.Table.TILTAKKODEVERK_V2;
import no.nav.pto.veilarbportefolje.domene.EnhetTiltak;
import no.nav.pto.veilarbportefolje.domene.Tiltakkodeverk;
import no.nav.pto.veilarbportefolje.domene.value.PersonId;
import no.nav.pto.veilarbportefolje.util.DateUtils;
import no.nav.pto.veilarbportefolje.util.DbUtils;
import no.nav.sbl.sql.SqlUtils;
import no.nav.sbl.sql.where.WhereClause;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.*;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static no.nav.pto.veilarbportefolje.arenapakafka.aktiviteter.ArenaAktivitetUtils.getDateOrNull;
import static no.nav.pto.veilarbportefolje.database.Table.BRUKERTILTAK_V2;
import static no.nav.pto.veilarbportefolje.database.Table.BRUKERTILTAK_V2.*;
import static no.nav.pto.veilarbportefolje.database.Table.OPPFOLGINGSBRUKER.PERSON_ID;
import static no.nav.pto.veilarbportefolje.postgres.PostgresUtils.queryForObjectOrNull;

@Slf4j
@Repository
@RequiredArgsConstructor
public class TiltakRepository {
    private final JdbcTemplate db;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final AktivitetDAO aktivitetDAO;

    public void upsert(TiltakInnhold innhold, AktorId aktorId) {
        Timestamp tilDato = Optional.ofNullable(getDateOrNull(innhold.getAktivitetperiodeTil(), true))
                .map(DateUtils::toTimestamp)
                .orElse(null);
        Timestamp fraDato = Optional.ofNullable(getDateOrNull(innhold.getAktivitetperiodeFra(), false))
                .map(DateUtils::toTimestamp)
                .orElse(null);

        log.info("Lagrer tiltak: {}", innhold.getAktivitetid());

        if (skalOppdatereTiltakskodeVerk(innhold.getTiltakstype(), innhold.getTiltaksnavn())) {
            SqlUtils.upsert(db, TILTAKKODEVERK_V2.TABLE_NAME)
                    .set(TILTAKKODEVERK_V2.KODE, innhold.getTiltakstype())
                    .set(TILTAKKODEVERK_V2.VERDI, innhold.getTiltaksnavn())
                    .where(WhereClause.equals(TILTAKKODEVERK_V2.KODE, innhold.getTiltakstype()))
                    .execute();
        }
        SqlUtils.upsert(db, TABLE_NAME)
                .set(AKTIVITETID, innhold.getAktivitetid())
                .set(PERSONID, String.valueOf(innhold.getPersonId()))
                .set(AKTOERID, aktorId.get())
                .set(TILTAKSKODE, innhold.getTiltakstype())
                .set(FRADATO, fraDato)
                .set(TILDATO, tilDato)
                .where(WhereClause.equals(AKTIVITETID, innhold.getAktivitetid()))
                .execute();

    }

    public void delete(String tiltakId) {
        log.info("Sletter tiltak: {}", tiltakId);
        SqlUtils.delete(db, TABLE_NAME)
                .where(WhereClause.equals(AKTIVITETID, tiltakId))
                .execute();
    }

    public EnhetTiltak hentTiltakPaEnhet(EnhetId enhetId) {
        final String hentTiltakPaEnhetSql = "SELECT * FROM " + TILTAKKODEVERK_V2.TABLE_NAME + " WHERE " +
                TILTAKKODEVERK_V2.KODE + " IN (SELECT DISTINCT " + TILTAKSKODE + " FROM " + BRUKERTILTAK_V2.TABLE_NAME +
                " BT INNER JOIN " + Table.OPPFOLGINGSBRUKER.TABLE_NAME + " OP ON BT." + PERSONID + " = OP." + PERSON_ID +
                " WHERE OP.NAV_KONTOR=:nav_kontor)";

        return new EnhetTiltak().setTiltak(
                namedParameterJdbcTemplate
                        .queryForList(hentTiltakPaEnhetSql, Map.of("nav_kontor", enhetId.get()))
                        .stream().map(this::mapTilTiltak)
                        .collect(toMap(Tiltakkodeverk::getKode, Tiltakkodeverk::getVerdi))
        );
    }

    public List<Timestamp> hentSluttdatoer(PersonId personId) {
        if (personId == null) {
            throw new IllegalArgumentException("Trenger personId for å hente ut sluttdatoer");
        }

        final String hentSluttDatoerSql = "SELECT " + TILDATO + " FROM " + TABLE_NAME +
                " WHERE " + PERSONID + "=?";
        return db.queryForList(hentSluttDatoerSql, Timestamp.class, personId.getValue());
    }


    public List<Timestamp> hentStartDatoer(PersonId personId) {
        if (personId == null) {
            throw new IllegalArgumentException("Trenger personId for å hente ut startdatoer");
        }

        final String hentStartDatoerSql = "SELECT " + FRADATO + " FROM " + TABLE_NAME +
                " WHERE " + PERSONID + "=?";
        return db.queryForList(hentStartDatoerSql, Timestamp.class, personId.getValue());
    }

    /*
    Kan forenkles til kun en bruker ved overgang til postgres
    String sql = "SELECT DISTINCT " + Table.BRUKERTILTAK_V2.TILTAKSKODE + " FROM " + Table.BRUKERTILTAK_V2.TABLE_NAME
            + " WHERE " + Table.BRUKERTILTAK_V2.AKTOERID + " = ?";
     */
    public Map<AktorId, Set<BrukertiltakV2>> hentBrukertiltak(List<AktorId> aktorIder) {
        if (aktorIder == null || aktorIder.isEmpty()) {
            throw new IllegalArgumentException("Trenger aktor-ider for å hente ut tiltak");
        }
        Map<String, Object> params = new HashMap<>();
        params.put("aktorIder", aktorIder.stream().map(AktorId::toString).collect(toList()));

        final String tiltaksTyperForListOfAktorIdsSql =
                "SELECT TILTAKSKODE, AKTOERID, TILDATO " +
                        "FROM  BRUKERTILTAK_V2 " +
                        "WHERE  AKTOERID in (:aktorIder)";
        return namedParameterJdbcTemplate
                .queryForList(tiltaksTyperForListOfAktorIdsSql, params)
                .stream()
                .map(this::mapTilBrukertiltakV2)
                .collect(toMap(BrukertiltakV2::getAktorId, DbUtils::toSet,
                        (oldValue, newValue) -> {
                            oldValue.addAll(newValue);
                            return oldValue;
                        }));
    }

    public void utledOgLagreTiltakInformasjon(AktorId aktorId, PersonId personId) {
        List<BrukertiltakV2> tiltak = hentTiltak(aktorId);
        LocalDate yesterday = LocalDate.now().minusDays(1);
        Timestamp nesteUtlopsdato = tiltak.stream()
                .map(BrukertiltakV2::getTildato)
                .filter(Objects::nonNull)
                .filter(utlopsdato -> utlopsdato.toLocalDateTime().toLocalDate().isAfter(yesterday))
                .min(Comparator.naturalOrder())
                .orElse(null);

        boolean aktiv = !tiltak.isEmpty();
        AktivitetStatus aktivitetStatus = new AktivitetStatus()
                .setAktivitetType(AktivitetTyper.tiltak.name())
                .setAktiv(aktiv)
                .setAktoerid(aktorId)
                .setPersonid(personId)
                .setNesteUtlop(nesteUtlopsdato);
        aktivitetDAO.upsertAktivitetStatus(aktivitetStatus);
    }

    private List<BrukertiltakV2> hentTiltak(AktorId aktorId) {
        String sql = "SELECT * FROM " + TABLE_NAME
                + " WHERE " + AKTOERID + " = ?";
        return db.queryForList(sql, aktorId.get())
                .stream()
                .map(this::mapTilBrukertiltakV2)
                .collect(toList());
    }

    private boolean skalOppdatereTiltakskodeVerk(String tiltaksKode, String verdiFraKafka) {
        Optional<String> verdiITiltakskodeVerk = hentVerdiITiltakskodeVerk(tiltaksKode);
        return verdiITiltakskodeVerk.map(lagretVerdi -> !lagretVerdi.equals(verdiFraKafka)).orElse(true);
    }

    private Optional<String> hentVerdiITiltakskodeVerk(String kode) {
        String sql = "SELECT " + TILTAKKODEVERK_V2.VERDI + " FROM " + TILTAKKODEVERK_V2.TABLE_NAME
                + " WHERE " + TILTAKKODEVERK_V2.KODE + " = ?";
        return Optional.ofNullable(
                queryForObjectOrNull(() -> db.queryForObject(sql, String.class, kode))
        );
    }


    @SneakyThrows
    private BrukertiltakV2 mapTilBrukertiltakV2(Map<String, Object> rs) {
        return new BrukertiltakV2()
                .setTiltak((String) rs.get(TILTAKSKODE))
                .setTildato((Timestamp) rs.get(TILDATO))
                .setAktorId(AktorId.of((String) rs.get(AKTOERID)));
    }

    @SneakyThrows
    private Tiltakkodeverk mapTilTiltak(Map<String, Object> rs) {
        return Tiltakkodeverk.of((String) rs.get(TILTAKKODEVERK_V2.KODE), (String) rs.get(TILTAKKODEVERK_V2.VERDI));
    }
}
