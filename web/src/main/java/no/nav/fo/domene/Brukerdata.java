package no.nav.fo.domene;

import lombok.Data;
import lombok.experimental.Accessors;
import no.nav.fo.util.sql.InsertQuery;
import no.nav.fo.util.sql.SqlUtils;
import no.nav.fo.util.sql.UpdateBatchQuery;
import no.nav.fo.util.sql.UpdateQuery;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Accessors(chain = true)
public class Brukerdata {
    private String aktoerid;
    private String veileder;
    private String personid;
    private Timestamp tildeltTidspunkt;
    private YtelseMapping ytelse;
    private LocalDateTime utlopsdato;
    private ManedMapping utlopsdatoFasett;
    private LocalDateTime aapMaxtid;
    private KvartalMapping aapMaxtidFasett;
    private Boolean oppfolging;

    public UpdateQuery toUpdateQuery(JdbcTemplate db) {
        return SqlUtils.update(db, "bruker_data")
                .set("VEILEDERIDENT", veileder)
                .set("TILDELT_TIDSPUNKT", tildeltTidspunkt)
                .set("AKTOERID", aktoerid)
                .set("YTELSE", ytelse != null ? ytelse.toString() : null)
                .set("UTLOPSDATO", toTimestamp(utlopsdato))
                .set("UTLOPSDATOFASETT", utlopsdatoFasett != null ? utlopsdatoFasett.toString() : null)
                .set("AAPMAXTID", toTimestamp(aapMaxtid))
                .set("AAPMAXTIDFASETT", aapMaxtidFasett != null ? aapMaxtidFasett.toString() : null)
                .set("OPPFOLGING", toJaNei(oppfolging))
                .whereEquals("PERSONID", personid);
    }

    public InsertQuery toInsertQuery(JdbcTemplate db) {
        return SqlUtils.insert(db, "bruker_data")
                .value("VEILEDERIDENT", veileder)
                .value("TILDELT_TIDSPUNKT", tildeltTidspunkt)
                .value("AKTOERID", aktoerid)
                .value("YTELSE", ytelse != null ? ytelse.toString() : null)
                .value("UTLOPSDATO", toTimestamp(utlopsdato))
                .value("UTLOPSDATOFASETT", utlopsdatoFasett != null ? utlopsdatoFasett.toString() : null)
                .value("AAPMAXTID", toTimestamp(aapMaxtid))
                .value("AAPMAXTIDFASETT", aapMaxtidFasett != null ? aapMaxtidFasett.toString() : null)
                .value("PERSONID", personid)
                .value("OPPFOLGING", toJaNei(oppfolging));

    }


    public static int[] batchUpdate(JdbcTemplate db, List<Brukerdata> data) {
        UpdateBatchQuery<Brukerdata> updateQuery = new UpdateBatchQuery<>(db, "bruker_data");

        return updateQuery
                .add("VEILEDERIDENT", Brukerdata::getVeileder, String.class)
                .add("TILDELT_TIDSPUNKT", (bruker) -> bruker.tildeltTidspunkt, Timestamp.class)
                .add("AKTOERID", Brukerdata::getAktoerid, String.class)
                .add("YTELSE", (bruker) -> safeToString(bruker.ytelse), String.class)
                .add("UTLOPSDATO", (bruker) -> toTimestamp(bruker.utlopsdato), Timestamp.class)
                .add("UTLOPSDATOFASETT", (bruker) -> safeToString(bruker.utlopsdatoFasett), String.class)
                .add("AAPMAXTID", (bruker) -> toTimestamp(bruker.aapMaxtid), Timestamp.class)
                .add("AAPMAXTIDFASETT", (bruker) -> safeToString(bruker.aapMaxtidFasett), String.class)
                .add("OPPFOLGING", (bruker) -> toJaNei(bruker.oppfolging), String.class)
                .addWhereClause("PERSONID", (bruker) -> bruker.personid)
                .execute(data);
    }

    private static String toJaNei(boolean oppfolging) {
        String s = oppfolging ? "J" : "N";
        return s;
    }

    private static Object safeToString(Object o) {
        return o != null ? o.toString() : null;
    }

    private static Timestamp toTimestamp(LocalDateTime localDateTime) {
        return localDateTime != null ? Timestamp.valueOf(localDateTime) : null;
    }
}
