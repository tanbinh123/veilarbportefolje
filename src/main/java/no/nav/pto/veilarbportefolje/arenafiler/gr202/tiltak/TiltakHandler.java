package no.nav.pto.veilarbportefolje.arenafiler.gr202.tiltak;

import io.micrometer.core.instrument.Gauge;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import no.nav.pto.veilarbportefolje.aktiviteter.AktivitetUtils;
import no.nav.pto.veilarbportefolje.arenafiler.ArenaFilType;
import no.nav.pto.veilarbportefolje.arenafiler.FilmottakConfig;
import no.nav.pto.veilarbportefolje.config.EnvironmentProperties;
import no.nav.pto.veilarbportefolje.domene.Brukerdata;
import no.nav.pto.veilarbportefolje.domene.value.PersonId;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static no.nav.pto.veilarbportefolje.arenafiler.FilmottakFileUtils.getLastModifiedTimeInMillis;
import static no.nav.pto.veilarbportefolje.arenafiler.FilmottakFileUtils.hoursSinceLastChanged;
import static no.nav.pto.veilarbportefolje.elastic.MetricsReporter.getMeterRegistry;

@Slf4j
public class TiltakHandler {

    private final EnvironmentProperties environmentProperties;

    static final String ARENA_AKTIVITET_DATOFILTER = "2017-12-04";

    public TiltakHandler(EnvironmentProperties environmentProperties) {
        this.environmentProperties = environmentProperties;
        Gauge.builder("portefolje_arena_fil_aktiviteter_sist_oppdatert", this::sjekkArenaAktiviteterSistOppdatert).register(getMeterRegistry());
    }

    public FilmottakConfig.SftpConfig lopendeAktiviteter() {
        return new FilmottakConfig.SftpConfig(environmentProperties.getArenaPaagaaendeAktiviteterUrl(),
                environmentProperties.getArenaFilmottakSFTPUsername(),
                environmentProperties.getArenaFilmottakSFTPPassword(),
                ArenaFilType.GR_199_TILTAK);
    }

    public long sjekkArenaAktiviteterSistOppdatert() {
        Long millis = getLastModifiedTimeInMillis(lopendeAktiviteter()).getOrElseThrow(() -> new RuntimeException());
        return hoursSinceLastChanged(LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of("Europe/Oslo")));
    }

    public static Timestamp getDatoFilter() {
        return AktivitetUtils.parseDato(ARENA_AKTIVITET_DATOFILTER);
    }

    static Brukerdata oppdaterBrukerDataOmNodvendig(Brukerdata brukerdata,
                                                    Map<PersonId, TiltakOppdateringer> tiltakOppdateringerFraTiltaksfil) {
        PersonId personId = PersonId.of(brukerdata.getPersonid());
        TiltakOppdateringer oppdateringer = tiltakOppdateringerFraTiltaksfil.get(personId);

        Set<LocalDate> startDatoer = Stream.of(
                Optional.ofNullable(oppdateringer.getAktivitetStart()),
                Optional.ofNullable(oppdateringer.getNesteAktivitetStart()),
                Optional.ofNullable(brukerdata.getAktivitetStart()),
                Optional.ofNullable(brukerdata.getNesteAktivitetStart())
        )
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(timestamp -> timestamp.toLocalDateTime().toLocalDate())
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Iterator<LocalDate> iterator = startDatoer.iterator();
        Timestamp aktivitetStart = Try
                .of(iterator::next)
                .map(time -> Timestamp.valueOf(time.atStartOfDay()))
                .getOrElse(() -> null);
        Timestamp nesteAktivitetStart = Try
                .of(iterator::next)
                .map(time -> Timestamp.valueOf(time.atStartOfDay()))
                .getOrElse(() -> null);


        brukerdata.setAktivitetStart(aktivitetStart);
        brukerdata.setNesteAktivitetStart(nesteAktivitetStart);

        Optional.ofNullable(oppdateringer.getForrigeAktivitetStart())
                .ifPresent(kanskjeNyDato ->
                        oppdaterMedNyesteDatofelt(
                                brukerdata::getForrigeAktivitetStart,
                                brukerdata::setForrigeAktivitetStart,
                                kanskjeNyDato));
        Optional.ofNullable(oppdateringer.getNyesteUtlopteAktivitet())
                .ifPresent(kanskjeNyDato ->
                        oppdaterMedNyesteDatofelt(brukerdata::getNyesteUtlopteAktivitet,
                                brukerdata::setNyesteUtlopteAktivitet,
                                kanskjeNyDato));
        return brukerdata;
    }

    private static void oppdaterMedNyesteDatofelt(Supplier<Timestamp> getDato, Consumer<Timestamp> setDate, Timestamp kanskjeNydato) {
        if (getDato.get() == null) {
            setDate.accept(kanskjeNydato);
        } else {
            setDate.accept(nyeste(getDato.get(), kanskjeNydato));
        }
    }


    private static Timestamp nyeste(Timestamp t1, Timestamp t2) {
        return t1.before(t2) ? t2 : t1;
    }
}
