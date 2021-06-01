package no.nav.pto.veilarbportefolje.oppfolging;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import no.nav.common.types.identer.AktorId;
import no.nav.pto.veilarbportefolje.domene.BrukerOppdatertInformasjon;
import no.nav.pto.veilarbportefolje.domene.value.VeilederId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.time.ZonedDateTime;
import java.util.Optional;

import static no.nav.pto.veilarbportefolje.database.PostgresTable.OPPFOLGING_DATA.*;
import static no.nav.pto.veilarbportefolje.postgres.PostgresUtils.queryForObjectOrNull;
import static no.nav.pto.veilarbportefolje.util.DateUtils.toTimestamp;
import static no.nav.pto.veilarbportefolje.util.DateUtils.toZonedDateTime;

@Slf4j
@Repository
public class OppfolgingRepositoryV2 {
    private final JdbcTemplate db;

    @Autowired
    public OppfolgingRepositoryV2(@Qualifier("PostgresJdbc") JdbcTemplate db) {
        this.db = db;
    }

    public int settUnderOppfolging(AktorId aktoerId, ZonedDateTime startDato) {
        return db.update(
                "INSERT INTO " + TABLE_NAME + " (" + AKTOERID + ", " + OPPFOLGING + ", " + STARTDATO + ") VALUES (?,?,?) " +
                        "ON CONFLICT (" + AKTOERID + ") DO UPDATE SET " + OPPFOLGING + "  = ?, " + STARTDATO + " = ?;",
                aktoerId.get(),
                true, toTimestamp(startDato),
                true, toTimestamp(startDato)
        );
    }

    public int settVeileder(AktorId aktorId, VeilederId veilederId) {
        log.info("Setter veileder for bruker: {}, til: {}, ufordelt: {}", aktorId.get(), veilederId.getValue());

        String sql = String.format("UPDATE %s SET %s = ? WHERE %s = ?", TABLE_NAME, VEILEDERID, AKTOERID);
        return db.update(sql, veilederId.getValue(), aktorId.get());
    }

    public int settUfordeltStatus(String aktoerId, boolean ufordelt) {
        if(aktoerId == null){
            return 0;
        }
        log.info("Setter ny ufordelt status for bruker: {}, til: {}", aktoerId, ufordelt);

        String sql = String.format("UPDATE %s SET %s = ? WHERE %s = ?", TABLE_NAME, ER_UFORDELT, AKTOERID);
        return db.update(sql, ufordelt, aktoerId);
    }

    public int settNyForVeileder(AktorId aktoerId, boolean nyForVeileder) {
        log.info("Setter ny for veileder til: {} for bruker : {}", nyForVeileder, aktoerId.get());

        String sql = String.format("UPDATE %s SET %s = ? WHERE %s = ?", TABLE_NAME, NY_FOR_VEILEDER, AKTOERID);
        return db.update(sql, nyForVeileder, aktoerId.get());
    }

    public int settManuellStatus(AktorId aktoerId, boolean manuellStatus) {
        log.info("Setter ny manuell status for bruker: {}, til: {}", aktoerId.get(), manuellStatus);

        String sql = String.format("UPDATE %s SET %s = ? WHERE %s = ?", TABLE_NAME, MANUELL, AKTOERID);
        return db.update(sql, manuellStatus, aktoerId.get());
    }

    public int settOppfolgingTilFalse(AktorId aktoerId) {
        log.info("Setter oppfolging til false for bruker: {}", aktoerId.get());

        String sql = String.format("UPDATE %s SET %s = ? WHERE %s = ?", TABLE_NAME, OPPFOLGING, AKTOERID);
        return db.update(sql, false, aktoerId.get());
    }

    public Optional<ZonedDateTime> hentStartdato(AktorId aktoerId) {
        String sql = String.format("SELECT %s FROM %s WHERE %s = ?", STARTDATO, TABLE_NAME, AKTOERID);
        return Optional.ofNullable(
                queryForObjectOrNull(() -> db.queryForObject(sql, (rs, row) -> toZonedDateTime(rs.getTimestamp(STARTDATO)), aktoerId.get()))
        );
    }

    public boolean hentUfordeltStatus(AktorId aktorId) {
        String sql = String.format("SELECT %s FROM %s WHERE %s = ?", ER_UFORDELT, TABLE_NAME, AKTOERID);
        return Optional.ofNullable(
                queryForObjectOrNull(() -> db.queryForObject(sql, (rs, row) -> rs.getBoolean(ER_UFORDELT), aktorId.get()))
        ).orElse(true);
    }

    public void slettOppfolgingData(AktorId aktoerId) {
        log.info("Sletter oppfolgings data for bruker: {}", aktoerId.get());
        db.update(String.format("DELETE FROM %s WHERE %s = ?", TABLE_NAME, AKTOERID), aktoerId.get());
    }

    public Optional<BrukerOppdatertInformasjon> hentOppfolgingData(AktorId aktoerId) {
        String sql = String.format("SELECT * FROM %s WHERE %s = ?", TABLE_NAME, AKTOERID);
        return Optional.ofNullable(
                queryForObjectOrNull(() -> db.queryForObject(sql, this::mapToBrukerOppdatertInformasjon, aktoerId.get()))
        );
    }

    @SneakyThrows
    private BrukerOppdatertInformasjon mapToBrukerOppdatertInformasjon(ResultSet rs, int row) {
        if (rs == null || rs.getString(AKTOERID) == null) {
            return null;
        }
        return new BrukerOppdatertInformasjon()
                .setAktoerid(rs.getString(AKTOERID))
                .setNyForVeileder(rs.getBoolean(NY_FOR_VEILEDER))
                .setOppfolging(rs.getBoolean(OPPFOLGING))
                .setVeileder(rs.getString(VEILEDERID))
                .setManuell(rs.getBoolean(MANUELL))
                .setStartDato(rs.getTimestamp(STARTDATO));
    }

}
