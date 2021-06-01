package no.nav.pto.veilarbportefolje.database;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class UfordeltBrukerDTO {
    String aktorId;
    String veileder;
}
