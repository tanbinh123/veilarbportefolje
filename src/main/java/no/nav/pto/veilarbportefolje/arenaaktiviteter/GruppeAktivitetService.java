package no.nav.pto.veilarbportefolje.arenaaktiviteter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.nav.common.types.identer.AktorId;
import no.nav.pto.veilarbportefolje.aktiviteter.AktivitetService;
import no.nav.pto.veilarbportefolje.aktiviteter.KafkaAktivitetMelding;
import no.nav.pto.veilarbportefolje.arenaaktiviteter.arenaDTO.GruppeAktivitetDTO;
import no.nav.pto.veilarbportefolje.arenaaktiviteter.arenaDTO.GruppeAktivitetInnhold;
import no.nav.pto.veilarbportefolje.domene.AktorClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import static no.nav.pto.veilarbportefolje.arenaaktiviteter.ArenaAktivitetUtils.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class GruppeAktivitetService {
    private final AktivitetService aktivitetService;
    private final AktorClient aktorClient;
    private final ArenaHendelseRepository arenaHendelseRepository;

    @Transactional
    public void behandleKafkaMelding(GruppeAktivitetDTO kafkaMelding) {
        log.info("Behandler utdannings-aktivtet-melding");
        GruppeAktivitetInnhold innhold = getInnhold(kafkaMelding);
        if (innhold == null || erGammelMelding(kafkaMelding, innhold)) {
            return;
        }

        AktorId aktorId = getAktorId(aktorClient, innhold.getFnr());
        if (skalSlettes(kafkaMelding)) {
            aktivitetService.slettAktivitet(innhold.getAktivitetid(), aktorId);
        } else {
            KafkaAktivitetMelding melding = mapTilKafkaAktivitetMelding(innhold, aktorId);
            aktivitetService.upsertOgIndekserAktiviteter(melding);
        }
    }

    private boolean erGammelMelding(GruppeAktivitetDTO kafkaMelding, GruppeAktivitetInnhold innhold ){
        Long hendelseIDB = arenaHendelseRepository.retrieveHendelse(innhold.getAktivitetid());

        if (erGammelHendelseBasertPaOperasjon(hendelseIDB, innhold.getHendelseId(), kafkaMelding.getOperationType())) {
            log.info("Fikk tilsendt gammel gruppe-aktivtet-melding");
            return true;
        }
        arenaHendelseRepository.upsertHendelse(innhold.getAktivitetid(), innhold.getHendelseId());
        return false;
    }

    private KafkaAktivitetMelding mapTilKafkaAktivitetMelding(GruppeAktivitetInnhold melding, AktorId aktorId) {
        if(melding == null || aktorId == null){
            return null;
        }
        KafkaAktivitetMelding kafkaAktivitetMelding = new KafkaAktivitetMelding();
        kafkaAktivitetMelding.setAktorId(aktorId.get());
        kafkaAktivitetMelding.setAktivitetId(melding.getAktivitetid()); //TODO: Sjekk om denne er unik i forhold til de andre
        kafkaAktivitetMelding.setFraDato(getDateOrNull(melding.getAktivitetperiodeFra()));
        kafkaAktivitetMelding.setTilDato(getDateOrNull(melding.getAktivitetperiodeTil(), true));
        kafkaAktivitetMelding.setEndretDato(getDateOrNull(melding.getEndretDato()));

        kafkaAktivitetMelding.setAktivitetStatus(KafkaAktivitetMelding.AktivitetStatus.GJENNOMFORES);
        kafkaAktivitetMelding.setAktivitetType(KafkaAktivitetMelding.AktivitetTypeData.GRUPPEAKTIVITET);
        kafkaAktivitetMelding.setAvtalt(true);
        kafkaAktivitetMelding.setHistorisk(false);
        kafkaAktivitetMelding.setVersion(-1L);

        return kafkaAktivitetMelding;
    }
}
