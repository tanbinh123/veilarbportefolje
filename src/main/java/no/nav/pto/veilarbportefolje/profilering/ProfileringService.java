package no.nav.pto.veilarbportefolje.profilering;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.nav.arbeid.soker.profilering.ArbeidssokerProfilertEvent;
import no.nav.common.types.identer.AktorId;
import no.nav.pto.veilarbportefolje.kafka.KafkaCommonConsumerService;
import no.nav.pto.veilarbportefolje.util.DateUtils;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProfileringService extends KafkaCommonConsumerService<ArbeidssokerProfilertEvent> {
    private final ProfileringRepository profileringRepository;
    private final ProfileringRepositoryV2 profileringRepositoryV2;

    public void behandleKafkaMeldingLogikk(ArbeidssokerProfilertEvent kafkaMelding) {
        profileringRepositoryV2.upsertBrukerProfilering(kafkaMelding);
        profileringRepository.upsertBrukerProfilering(kafkaMelding);

        log.info("Oppdaterer brukerprofilering i postgres for: {}, {}, {}", kafkaMelding.getAktorid(), kafkaMelding.getProfilertTil().name(), DateUtils.zonedDateStringToTimestamp(kafkaMelding.getProfileringGjennomfort()));
    }


    public void migrerTilPostgres() {
        List<AktorId> alleBrukereMedprofilering = profileringRepository.hentAlleBrukereMedProfileringer();
        log.info("Migrering av profilering for {} brukere.", alleBrukereMedprofilering.size());
        alleBrukereMedprofilering.forEach(bruker -> {
                    try {
                        ArbeidssokerProfilertEvent brukerProfilering = profileringRepository.hentBrukerProfilering(bruker);
                        if (brukerProfilering != null) {
                            profileringRepositoryV2.upsertBrukerProfilering(brukerProfilering);
                        }
                    } catch (Exception e) {
                        log.error("Migrering feilet på bruker: {}", bruker, e);
                    }
                }
        );

        int brukereMedprofileringOracle = profileringRepository.hentAlleBrukereMedProfileringer().size();
        int brukereMedprofileringPostgres = profileringRepositoryV2.hentAlleBrukereMedProfileringer().size();
        log.info("Migrering av profilering er ferdig. Brukere i oracle: {}, i Postgres: {}", brukereMedprofileringOracle, brukereMedprofileringPostgres);
    }

}
