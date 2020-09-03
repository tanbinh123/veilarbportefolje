package no.nav.pto.veilarbportefolje.oppfolging;

import lombok.Value;
import no.nav.pto.veilarbportefolje.domene.AktoerId;

import java.time.LocalDateTime;

@Value
public class OppfolgingAvsluttetDTO {
    AktoerId aktorId;
    LocalDateTime sluttdato;
}
