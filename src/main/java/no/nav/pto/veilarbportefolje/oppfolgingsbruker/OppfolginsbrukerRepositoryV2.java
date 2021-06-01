package no.nav.pto.veilarbportefolje.oppfolgingsbruker;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import no.nav.common.types.identer.AktorId;
import no.nav.common.types.identer.EnhetId;
import no.nav.pto.veilarbportefolje.domene.value.NavKontor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.time.ZonedDateTime;
import java.util.Optional;

import static no.nav.pto.veilarbportefolje.postgres.PostgresUtils.queryForObjectOrNull;
import static no.nav.pto.veilarbportefolje.util.DateUtils.toZonedDateTime;
import static no.nav.pto.veilarbportefolje.database.PostgresTable.OPPFOLGINGSBRUKER_ARENA.*;

@Slf4j
@Repository
public class OppfolginsbrukerRepositoryV2 {
    private final JdbcTemplate db;

    @Autowired
    public OppfolginsbrukerRepositoryV2(@Qualifier("PostgresJdbc") JdbcTemplate db) {
        this.db = db;
    }

    public int leggTilEllerEndreOppfolgingsbruker(OppfolgingsbrukerKafkaDTO oppfolgingsbruker) {
        if (oppfolgingsbruker == null || oppfolgingsbruker.getAktoerid() == null) {
            return 0;
        }

        Optional<ZonedDateTime> sistEndretDato = getEndretDato(oppfolgingsbruker.getAktoerid());
        if (oppfolgingsbruker.getEndret_dato() == null || (sistEndretDato.isPresent() && sistEndretDato.get().isAfter(oppfolgingsbruker.getEndret_dato()))) {
            log.info("Oppdaterer ikke oppfolgingsbruker: {}", oppfolgingsbruker.getAktoerid());
            return 0;
        }

        return db.update("INSERT INTO " + TABLE_NAME
                + " (" + SQLINSERT_STRING + ") " +
                "VALUES(" + oppfolgingsbruker.toSqlInsertString() + ") " +
                "ON CONFLICT (" + AKTOERID + ") DO UPDATE SET (" + SQLUPDATE_STRING + ") = (" + oppfolgingsbruker.toSqlUpdateString() + ")");
    }

    public Optional<OppfolgingsbrukerKafkaDTO> getOppfolgingsBruker(AktorId aktorId) {
        String sql = String.format("SELECT * FROM %s WHERE %s=? ", TABLE_NAME, AKTOERID);
        return Optional.ofNullable(
                queryForObjectOrNull(() -> db.queryForObject(sql, this::mapTilOppfolgingsbruker, aktorId.get()))
        );
    }


    private Optional<ZonedDateTime> getEndretDato(String aktorId) {
        String sql = String.format("SELECT %s FROM %s WHERE %s=? ", ENDRET_DATO, TABLE_NAME, AKTOERID);
        return Optional.ofNullable(
                queryForObjectOrNull(() -> db.queryForObject(sql, this::mapTilZonedDateTime, aktorId))
        );
    }

    @SneakyThrows
    private ZonedDateTime mapTilZonedDateTime(ResultSet rs, int row) {
        return toZonedDateTime(rs.getTimestamp(ENDRET_DATO));
    }

    @SneakyThrows
    private OppfolgingsbrukerKafkaDTO mapTilOppfolgingsbruker(ResultSet rs, int row) {
        if (rs == null || rs.getString(AKTOERID) == null) {
            return null;
        }
        return new OppfolgingsbrukerKafkaDTO()
                .setAktoerid(rs.getString(AKTOERID))
                .setFodselsnr(rs.getString(FODSELSNR))
                .setFormidlingsgruppekode(rs.getString(FORMIDLINGSGRUPPEKODE))
                .setIserv_fra_dato(toZonedDateTime(rs.getTimestamp(ISERV_FRA_DATO)))
                .setEtternavn(rs.getString(ETTERNAVN))
                .setFornavn(rs.getString(FORNAVN))
                .setNav_kontor(rs.getString(NAV_KONTOR))
                .setKvalifiseringsgruppekode(rs.getString(KVALIFISERINGSGRUPPEKODE))
                .setRettighetsgruppekode(rs.getString(RETTIGHETSGRUPPEKODE))
                .setHovedmaalkode(rs.getString(HOVEDMAALKODE))
                .setSikkerhetstiltak_type_kode(rs.getString(SIKKERHETSTILTAK_TYPE_KODE))
                .setFr_kode(rs.getString(DISKRESJONSKODE))
                .setHar_oppfolgingssak(rs.getBoolean(HAR_OPPFOLGINGSSAK))
                .setSperret_ansatt(rs.getBoolean(SPERRET_ANSATT))
                .setEr_doed(rs.getBoolean(ER_DOED))
                .setDoed_fra_dato(toZonedDateTime(rs.getTimestamp(DOED_FRA_DATO)))
                .setEndret_dato(toZonedDateTime(rs.getTimestamp(ENDRET_DATO)));
    }
}
