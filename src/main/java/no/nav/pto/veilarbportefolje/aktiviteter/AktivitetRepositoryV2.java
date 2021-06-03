package no.nav.pto.veilarbportefolje.aktiviteter;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.util.Optional;

import no.nav.common.types.identer.AktorId;

import static no.nav.pto.veilarbportefolje.postgres.PostgresUtils.queryForObjectOrNull;
import static no.nav.pto.veilarbportefolje.database.PostgresTable.AKTIVITETER.*;
import static no.nav.pto.veilarbportefolje.util.DateUtils.toTimestamp;

@Slf4j
@Repository
public class AktivitetRepositoryV2 {
    private final JdbcTemplate db;

    @Autowired
    public AktivitetRepositoryV2(@Qualifier("PostgresJdbc") JdbcTemplate db) {
        this.db = db;
    }

    @Transactional
    public void lagreAktivitetData(KafkaAktivitetMelding aktivitet) {
        try {
            if (aktivitet.isHistorisk()) {
                deleteById(aktivitet.getAktivitetId());
            } else if (erNyVersjonAvAktivitet(aktivitet)) {
                upsertAktivitet(aktivitet);
            }
        } catch (Exception e) {
            String message = String.format("Kunne ikke lagre aktivitetdata fra topic for aktivitetid %s", aktivitet.getAktivitetId());
            log.error(message, e);
        }
    }

    private void deleteById(String aktivitetid) {
        log.info("Sletter alle aktiviteter med id {}", aktivitetid);
        String sql = String.format("DELETE FROM %s WHERE %s = ?", TABLE_NAME, AKTIVITETID);
        db.update(sql, aktivitetid);
    }

    private boolean erNyVersjonAvAktivitet(KafkaAktivitetMelding aktivitet) {
        Long kommendeVersjon = aktivitet.getVersion();
        if (kommendeVersjon == null) {
            return false;
        }
        Long databaseVersjon = getVersjon(aktivitet.getAktivitetId());
        if (databaseVersjon == null) {
            return true;
        }
        return kommendeVersjon.compareTo(databaseVersjon) > 0;
    }

    public Optional<AktorId> getAktorId(String aktivitetId) {
        String sql = String.format("SELECT * FROM %s WHERE %s = ?", TABLE_NAME, AKTIVITETID);
        Optional<String> aktoerId = Optional.ofNullable(queryForObjectOrNull(() -> db.queryForObject(sql, (ResultSet rs, int rows) -> rs.getString(AKTOERID), aktivitetId)));

        return aktoerId.map(AktorId::of);
    }

    private Long getVersjon(String aktivitetId) {
        String sql = String.format("SELECT * FROM %s WHERE %s = ?", TABLE_NAME, AKTIVITETID);
        return db.queryForObject(sql, (ResultSet rs, int rows) -> rs.getLong(VERSION), aktivitetId);
    }

    public void upsertAktivitet(KafkaAktivitetMelding aktivitet) {
        db.update("INSERT INTO " + TABLE_NAME +
                        " (" + AKTIVITETID +
                        ", " + AKTOERID +
                        ", " + AKTIVITETTYPE +
                        ", " + AVTALT +
                        ", " + FRADATO +
                        ", " + TILDATO +
                        ", " + OPPDATERTDATO +
                        ", " + STATUS +
                        ", " + VERSION + ") " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (" + AKTIVITETID + ") DO UPDATE SET" +
                        " (" + AKTOERID +
                        ", " + AKTIVITETTYPE +
                        ", " + AVTALT +
                        ", " + FRADATO +
                        ", " + TILDATO +
                        ", " + OPPDATERTDATO +
                        ", " + STATUS +
                        ", " + VERSION + ") = (?, ?, ?, ?, ?, ?, ?, ?)",
                aktivitet.getAktivitetId(),
                aktivitet.getAktorId(), aktivitet.getAktivitetType().name().toLowerCase(), aktivitet.isAvtalt(),
                toTimestamp(aktivitet.getFraDato()), toTimestamp(aktivitet.getTilDato()), toTimestamp(aktivitet.getEndretDato()),
                aktivitet.getAktivitetStatus().name().toLowerCase(),
                aktivitet.getVersion(),

                aktivitet.getAktorId(), aktivitet.getAktivitetType().name().toLowerCase(), aktivitet.isAvtalt(),
                toTimestamp(aktivitet.getFraDato()), toTimestamp(aktivitet.getTilDato()), toTimestamp(aktivitet.getEndretDato()),
                aktivitet.getAktivitetStatus().name().toLowerCase(),
                aktivitet.getVersion()
        );
    }
}
