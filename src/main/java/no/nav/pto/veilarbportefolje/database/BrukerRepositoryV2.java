package no.nav.pto.veilarbportefolje.database;

import com.sun.el.stream.Stream;
import no.nav.common.types.identer.AktorId;
import no.nav.common.types.identer.EnhetId;
import no.nav.pto.veilarbportefolje.domene.Bruker;
import no.nav.pto.veilarbportefolje.oppfolgingsbruker.OppfolgingsbrukerKafkaDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

import static no.nav.pto.veilarbportefolje.database.PostgresTable.BRUKER_VIEW.*;
import static no.nav.pto.veilarbportefolje.postgres.PostgresUtils.queryForObjectOrNull;

@Repository
public class BrukerRepositoryV2 {
    private final JdbcTemplate db;

    @Autowired
    public BrukerRepositoryV2(@Qualifier("PostgresJdbc") JdbcTemplate db) {
        this.db = db;
    }

    public Optional<String> getNavKontor(AktorId aktorId) {
        String sql = String.format("SELECT %s FROM %s WHERE %s=? ", NAV_KONTOR, TABLE_NAME, AKTOERID);
        return Optional.ofNullable(
                queryForObjectOrNull(() -> db.queryForObject(sql, (rs, row) -> rs.getString(NAV_KONTOR), aktorId.get()))
        );
    }

    public Optional<List<Bruker>> getOppfolgingsBrukerePaEnhet(EnhetId enhetId) {
       return null;/*
        String sql = String.format("SELECT * FROM %s WHERE %s=?", TABLE_NAME, NAV_KONTOR);
        return Optional.ofNullable(
                //queryForObjectOrNull(() -> db.queryForList(sql, (rs, row) -> Bruker.of(rs), enhetId.get()))
        );*/
    }
}
