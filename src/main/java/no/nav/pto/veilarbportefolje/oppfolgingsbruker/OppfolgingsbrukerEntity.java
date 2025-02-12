package no.nav.pto.veilarbportefolje.oppfolgingsbruker;

import java.time.ZonedDateTime;

public record OppfolgingsbrukerEntity(String aktoerid, String fodselsnr, String formidlingsgruppekode,
                                      ZonedDateTime iserv_fra_dato, String etternavn, String fornavn,
                                      String nav_kontor, String kvalifiseringsgruppekode, String rettighetsgruppekode,
                                      String hovedmaalkode, String sikkerhetstiltak_type_kode, String fr_kode,
                                      boolean har_oppfolgingssak, boolean sperret_ansatt, boolean er_doed,
                                      ZonedDateTime doed_fra_dato, ZonedDateTime endret_dato) {
}
