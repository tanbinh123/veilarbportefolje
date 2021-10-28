package no.nav.pto.veilarbportefolje.aktiviteter;

import no.nav.common.types.identer.AktorId;
import no.nav.pto.veilarbportefolje.config.ApplicationConfigTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.ZonedDateTime;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@SpringBootTest(classes = ApplicationConfigTest.class)
public class AktiviteterV2 {
    private final AktivitetStatusRepositoryV2 aktivitetStatusRepositoryV2;
    private final AktiviteterRepositoryV2 aktiviteterRepositoryV2;
    private final AktivitetService aktivitetService;

    private final AktorId aktorId = AktorId.of("1000123");

    @Autowired
    public AktiviteterV2(AktivitetStatusRepositoryV2 aktivitetStatusRepositoryV2, AktiviteterRepositoryV2 aktiviteterRepositoryV2, AktivitetService aktivitetService) {
        this.aktivitetStatusRepositoryV2 = aktivitetStatusRepositoryV2;
        this.aktiviteterRepositoryV2 = aktiviteterRepositoryV2;
        this.aktivitetService = aktivitetService;
    }

    @Test
    public void skal_komme_i_aktivitet_V2() {
        KafkaAktivitetMelding aktivitet = new KafkaAktivitetMelding()
                .setAktivitetId("id1")
                .setVersion(1L)
                .setAktivitetType(KafkaAktivitetMelding.AktivitetTypeData.EGEN)
                .setAktorId(aktorId.get())
                .setAvtalt(true)
                .setEndretDato(ZonedDateTime.parse("2017-02-03T10:10:10+02:00"))
                .setAktivitetStatus(KafkaAktivitetMelding.AktivitetStatus.GJENNOMFORES);

        aktivitetService.lagreOgProsseseserAktiviteter(aktivitet);
        AktoerAktiviteter avtalteAktiviteterForAktoerid = aktiviteterRepositoryV2.getAktiviteterForAktoerid(aktorId, false);
        Optional<AktivitetStatus> aktivitettypeStatus = aktivitetStatusRepositoryV2.hentAktivitetTypeStatus(aktorId.get(), "egen");

        assertThat(avtalteAktiviteterForAktoerid.getAktiviteter().size()).isEqualTo(1);
        assertThat(aktivitettypeStatus).isPresent();
        assertThat(aktivitettypeStatus.get().aktiv).isTrue();
    }

    @Test
    public void skal_kunne_ha_flere_typer_aktiviteter_V2() {
        KafkaAktivitetMelding aktivitet1 = new KafkaAktivitetMelding()
                .setAktivitetId("id1")
                .setVersion(1L)
                .setAktivitetType(KafkaAktivitetMelding.AktivitetTypeData.EGEN)
                .setAktorId(aktorId.get())
                .setAvtalt(true)
                .setEndretDato(ZonedDateTime.parse("2017-02-03T10:10:10+02:00"))
                .setAktivitetStatus(KafkaAktivitetMelding.AktivitetStatus.GJENNOMFORES);

        KafkaAktivitetMelding aktivitet2 = new KafkaAktivitetMelding()
                .setAktivitetId("id2")
                .setVersion(1L)
                .setAktivitetType(KafkaAktivitetMelding.AktivitetTypeData.MOTE)
                .setAktorId(aktorId.get())
                .setAvtalt(true)
                .setEndretDato(ZonedDateTime.parse("2017-02-03T10:10:10+02:00"))
                .setAktivitetStatus(KafkaAktivitetMelding.AktivitetStatus.GJENNOMFORES);

        aktivitetService.lagreOgProsseseserAktiviteter(aktivitet1);
        aktivitetService.lagreOgProsseseserAktiviteter(aktivitet2);
        AktoerAktiviteter avtalteAktiviteterForAktoerid = aktiviteterRepositoryV2.getAktiviteterForAktoerid(aktorId, true);
        Optional<AktivitetStatus> aktivitettypeStatus1 = aktivitetStatusRepositoryV2.hentAktivitetTypeStatus(aktorId.get(), "egen");
        Optional<AktivitetStatus> aktivitettypeStatus2 = aktivitetStatusRepositoryV2.hentAktivitetTypeStatus(aktorId.get(), "mote");

        assertThat(avtalteAktiviteterForAktoerid.getAktiviteter().size()).isEqualTo(2);
        assertThat(aktivitettypeStatus1).isPresent();
        assertThat(aktivitettypeStatus1.get().aktiv).isTrue();
        assertThat(aktivitettypeStatus2).isPresent();
        assertThat(aktivitettypeStatus2.get().aktiv).isTrue();
    }
}