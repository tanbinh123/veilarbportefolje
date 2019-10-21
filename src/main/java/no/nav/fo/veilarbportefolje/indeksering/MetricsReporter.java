package no.nav.fo.veilarbportefolje.indeksering;

import io.micrometer.core.instrument.Gauge;
import lombok.extern.slf4j.Slf4j;
import no.nav.metrics.Event;
import no.nav.metrics.MetricsFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static no.nav.metrics.MetricsFactory.getMeterRegistry;

@Component
@Slf4j
public class MetricsReporter {

    private ElasticIndexer elasticIndexer;

    @Inject
    public MetricsReporter(ElasticIndexer elasticIndexer) {
        this.elasticIndexer = elasticIndexer;
        Gauge.builder("veilarbelastic_number_of_docs", ElasticUtils::getCount).register(getMeterRegistry());
        new ScheduledThreadPoolExecutor(2).scheduleAtFixedRate(this::sjekkIndeksSistOpprettet, 10, 10, MINUTES);
    }

    private void sjekkIndeksSistOpprettet() {
        String indeksNavn = elasticIndexer.hentGammeltIndeksNavn().orElseThrow(IllegalStateException::new);
        Event event = MetricsFactory.createEvent("portefolje.indeks.sist.opprettet");
        event.addFieldToReport("timestamp", hentIndekseringsdato(indeksNavn));
        event.report();
    }

    static LocalDateTime hentIndekseringsdato(String indeksNavn) {
        String[] split = indeksNavn.split("_");
        String klokkeslett = asList(split).get(split.length - 1);
        String dato = asList(split).get(split.length - 2);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
        return LocalDateTime.parse(dato + "_" + klokkeslett, formatter);
    }
}
