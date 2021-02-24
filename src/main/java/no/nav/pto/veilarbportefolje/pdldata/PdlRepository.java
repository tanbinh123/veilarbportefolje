package no.nav.pto.veilarbportefolje.pdldata;

import lombok.extern.slf4j.Slf4j;
import no.nav.arbeid.soker.profilering.ArbeidssokerProfilertEvent;
import no.nav.arbeid.soker.profilering.ProfilertTil;
import no.nav.common.types.identer.AktorId;
import no.nav.pto.veilarbportefolje.elastic.domene.OppfolgingsBruker;
import no.nav.pto.veilarbportefolje.util.DateUtils;
import no.nav.sbl.sql.SqlUtils;
import no.nav.sbl.sql.where.WhereClause;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.time.LocalDate;
import java.util.List;

import static java.time.format.DateTimeFormatter.ISO_ZONED_DATE_TIME;
import static no.nav.common.utils.CollectionUtils.partition;
import static no.nav.pto.veilarbportefolje.database.Table.PDL_DATA.*;
import static no.nav.pto.veilarbportefolje.util.DateUtils.toLocalDateTimeOrNull;
import static no.nav.pto.veilarbportefolje.util.DateUtils.toZonedDateTime;

@Slf4j
@Repository
public class PdlRepository {
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public PdlRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void upsert(AktorId aktorId, LocalDate fodseldag) {
        SqlUtils.upsert(jdbcTemplate, TABLE_NAME)
                .set(AKTOERID, aktorId.get())
                .set(FODSELSDAG, fodseldag)
                .where(WhereClause.equals(AKTOERID, aktorId.get()))
                .execute();
    }

    public void saveBatch(List<OppfolgingsBruker> oppfolgingsBrukerList) {
        final int batchSize = 1000;
        jdbcTemplate.execute("truncate table "+TABLE_NAME);
        partition(oppfolgingsBrukerList, batchSize).forEach(brukerBatch -> {
            jdbcTemplate.batchUpdate("INSERT INTO "+TABLE_NAME+" ("+AKTOERID+", "+FODSELSDAG+") values(?,?) ON CONFLICT DO NOTHING",
                    new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i)
                                throws SQLException {
                            OppfolgingsBruker employee = brukerBatch.get(i);
                            ps.setString(1, employee.getAktoer_id());
                            ps.setTime(2, DateUtils.dateToTime(employee.getFodselsdato()));
                        }

                        @Override
                        public int getBatchSize() {
                            return brukerBatch.size();
                        }
                    }
            );
        });
    }

    public LocalDate hentFodselsdag(AktorId aktorId) {
        return SqlUtils.select(jdbcTemplate, TABLE_NAME, (rs) -> rs.getDate(FODSELSDAG).toLocalDate())
                .column("*")
                .where(WhereClause.equals("AKTOERID", aktorId.get()))
                .execute();
    }

    public void slettPdlData(AktorId aktorId) {
        SqlUtils.delete(jdbcTemplate, TABLE_NAME)
                .where(WhereClause.equals(AKTOERID, aktorId.get())).execute();
    }

}
