package no.nav.pto.veilarbportefolje.domene;

import lombok.Value;
import no.nav.melding.virksomhet.tiltakogaktiviteterforbrukere.v1.Aktivitetstyper;
import no.nav.melding.virksomhet.tiltakogaktiviteterforbrukere.v1.Tiltakstyper;
import no.nav.sbl.sql.InsertBatchQuery;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

@Value(staticConstructor = "of")
public final class Tiltakkodeverk {
    private final String kode;
    private final String verdi;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        Tiltakkodeverk that = (Tiltakkodeverk) o;

        return kode.toLowerCase().equals(that.kode.toLowerCase());
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + kode.hashCode();
        return result;
    }

    public static Tiltakkodeverk of(Tiltakstyper tiltakstyper) {
        return Tiltakkodeverk.of(tiltakstyper.getValue(), tiltakstyper.getTermnavn());
    }

    public static Tiltakkodeverk of(Aktivitetstyper aktivitetstyper) {
        return Tiltakkodeverk.of(aktivitetstyper.getValue(), aktivitetstyper.getTermnavn());
    }

    public static int[] batchInsert(JdbcTemplate db, List<Tiltakkodeverk> data) {
        InsertBatchQuery<Tiltakkodeverk> insertQuery = new InsertBatchQuery<>(db, "tiltakkodeverk");

        return insertQuery
                .add("kode", Tiltakkodeverk::getKode, String.class)
                .add("verdi", Tiltakkodeverk::getVerdi, String.class)
                .execute(data);
    }

    public Tiltakkodeverk withKode(String kode) {
        return this.kode == kode ? this : new Tiltakkodeverk(kode, this.verdi);
    }

    public Tiltakkodeverk withVerdi(String verdi) {
        return this.verdi == verdi ? this : new Tiltakkodeverk(this.kode, verdi);
    }
}
