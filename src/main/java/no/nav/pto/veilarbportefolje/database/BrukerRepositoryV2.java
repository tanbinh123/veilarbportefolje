package no.nav.pto.veilarbportefolje.database;

import no.nav.common.types.identer.AktorId;
import no.nav.common.types.identer.EnhetId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toList;
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

    public Optional<List<UfordeltBrukerDTO>> getUfordeltBrukerDTOerPaEnhet(EnhetId enhetId) {
        String sql = String.format("SELECT %s, %s FROM %s WHERE %s=? ", AKTOERID, VEILEDERID, TABLE_NAME, NAV_KONTOR);
        List<Map<String, Object>> resultat = db.queryForList(sql, enhetId.get());
        if (resultat.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(resultat.stream()
                .map(r -> new UfordeltBrukerDTO().setAktorId((String) r.get(AKTOERID)).setVeileder((String) r.get(VEILEDERID)))
                .collect(toList()));
    }
}
