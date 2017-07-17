package no.nav.fo.domene;

import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.solr.common.SolrDocument;

import java.time.ZonedDateTime;
import java.util.Date;

import static no.nav.fo.util.DateUtils.toZonedDateTime;


@Data
@Accessors(chain = true)
@Getter
public class Arbeidsliste {
    final VeilederId sistEndretAv;
    final ZonedDateTime endringstidspunkt;
    final String kommentar;
    final ZonedDateTime frist;
    Boolean isOppfolgendeVeileder;

    public static Arbeidsliste of(SolrDocument brukerDokument) {

        VeilederId sistEndretAv = new VeilederId((String) brukerDokument.get("arbeidsliste_sistendretavveileder"));
        ZonedDateTime endringstidspunkt = toZonedDateTime((Date) brukerDokument.get("arbeidsliste_endringstidspunkt"));
        String kommentar = (String) brukerDokument.get("arbeidsliste_kommentar");
        ZonedDateTime frist = toZonedDateTime((Date) brukerDokument.get("arbeidsliste_frist"));
        Boolean isOppfolgendeVeileder = (Boolean) brukerDokument.get("arbeidsliste_er_oppfolgende_veileder");

        return new Arbeidsliste(sistEndretAv, endringstidspunkt, kommentar, frist)
                .setIsOppfolgendeVeileder(isOppfolgendeVeileder);
    }
}
