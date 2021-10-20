package no.nav.pto.veilarbportefolje.database;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.nav.common.types.identer.AktorId;
import no.nav.pto.veilarbportefolje.aktiviteter.AktivitetService;
import no.nav.pto.veilarbportefolje.arenapakafka.aktiviteter.GruppeAktivitetRepository;
import no.nav.pto.veilarbportefolje.arenapakafka.aktiviteter.TiltakRepository;
import no.nav.pto.veilarbportefolje.domene.value.PersonId;
import no.nav.pto.veilarbportefolje.oppfolging.OppfolgingRepository;
import no.nav.pto.veilarbportefolje.service.BrukerService;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class BrukerAktiviteterService {
    private final AktivitetService aktivitetService;
    private final TiltakRepository tiltakRepository;
    private final OppfolgingRepository oppfolgingRepository;
    private final GruppeAktivitetRepository gruppeAktivitetRepository;
    private final BrukerService brukerService;

    public void syncAktivitetOgBrukerData() {
        log.info("Starter jobb: oppdater BrukerAktiviteter og BrukerData");
        List<AktorId> brukereSomMaOppdateres = oppfolgingRepository.hentAlleBrukereUnderOppfolging();
        log.info("Oppdaterer brukerdata for alle brukere under oppfolging: {}", brukereSomMaOppdateres.size());
        syncAktivitetOgBrukerData(brukereSomMaOppdateres);
        log.info("Avslutter jobb: oppdater BrukerAktiviteter og BrukerData");
    }

    public void syncAktivitetOgBrukerData(List<AktorId> brukere) {
        ForkJoinPool pool = new ForkJoinPool(6);
        try {
            pool.submit(() ->
                    brukere.parallelStream().forEach(aktorId -> {
                        log.info("Oppdater BrukerAktiviteter og BrukerData for aktorId: {}", aktorId);
                                if (aktorId != null) {
                                    try {
                                        PersonId personId = brukerService.hentPersonidFraAktoerid(aktorId).toJavaOptional().orElse(null);
                                        syncAktiviteterOgBrukerData(personId, aktorId);
                                    } catch (Exception e) {
                                        log.warn("Fikk error under sync jobb, men fortsetter. Aktoer: {}, exception: {}", aktorId, e);
                                    }
                                }
                            }
                    )).get(8, TimeUnit.HOURS);
        } catch (Exception e) {
            log.error("Error i sync jobben.", e);
        }
    }

    public void syncAktivitetOgBrukerData(AktorId aktorId) {
        PersonId personId = brukerService.hentPersonidFraAktoerid(aktorId).toJavaOptional().orElse(null);
        if (personId == null) {
            log.info("Fant ingen personId pa aktor: {}", aktorId);
        }
        syncAktiviteterOgBrukerData(personId, aktorId);
    }

    public void syncAktiviteterOgBrukerData(PersonId personId, AktorId aktorId) {
        if(personId == null){
            // TODO: check om utdatert
            log.warn("AktoerId ble ikke oppdatert da personId er null: {}. Inaktiv aktorId?", aktorId.get());
            return;
        }
        tiltakRepository.utledOgLagreTiltakInformasjon(aktorId, personId);
        gruppeAktivitetRepository.utledOgLagreGruppeaktiviteter(aktorId, personId);
        aktivitetService.deaktiverUtgatteUtdanningsAktivteter(aktorId);
        aktivitetService.utledOgIndekserAktivitetstatuserForAktoerid(aktorId);
    }
}
