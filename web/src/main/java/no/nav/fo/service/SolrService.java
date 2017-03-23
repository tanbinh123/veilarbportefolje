package no.nav.fo.service;

import javaslang.control.Try;
import no.nav.fo.database.BrukerRepository;
import no.nav.fo.domene.Bruker;
import no.nav.fo.domene.FacetResults;
import no.nav.fo.domene.Filtervalg;
import no.nav.fo.exception.SolrUpdateResponseCodeException;
import no.nav.fo.util.DbUtils;
import no.nav.fo.util.SolrUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import javax.inject.Inject;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.transaction.annotation.Isolation.SERIALIZABLE;

public class SolrService {

    private static final Logger logger = getLogger(SolrService.class);

    private static final String HOVEDINDEKSERING = "Hovedindeksering";
    private static final String DELTAINDEKSERING = "Deltaindeksering";

    @Inject
    private SolrClient solrClient;

    @Inject
    private BrukerRepository brukerRepository;

    @Scheduled(cron = "${veilarbportefolje.cron.hovedindeksering}")
    @Transactional
    public void hovedindeksering() {
        if (SolrUtils.isSlaveNode()) {
            logger.info("Noden er en slave. Kun masternoden kan iverksett indeksering. Avbryter.");
            return;
        }

        logger.info("Starter hovedindeksering");
        LocalDateTime t0 = LocalDateTime.now();

        List<SolrInputDocument> dokumenter = brukerRepository.retrieveAlleBrukere();
        deleteAllDocuments();
        addDocuments(dokumenter);
        commit();
        brukerRepository.updateTidsstempel(Timestamp.valueOf(t0));

        logFerdig(t0, dokumenter.size(), HOVEDINDEKSERING);
    }


    @Scheduled(cron = "${veilarbportefolje.cron.deltaindeksering}")
    @Transactional
    public void deltaindeksering() {
        if (SolrUtils.isSlaveNode()) {
            logger.info("Noden er en slave. Kun masternoden kan iverksett indeksering. Avbryter.");
            return;
        }

        logger.info("Starter deltaindeksering");
        LocalDateTime t0 = LocalDateTime.now();
        Timestamp timestamp = Timestamp.valueOf(t0);

        List<SolrInputDocument> dokumenter = brukerRepository.retrieveOppdaterteBrukere();
        if (dokumenter.isEmpty()) {
            logger.info("Ingen nye dokumenter i databasen");
            return;
        }

        addDocuments(dokumenter);
        brukerRepository.updateTidsstempel(timestamp);
        commit();

        logFerdig(t0, dokumenter.size(), DELTAINDEKSERING);
    }

    public List<Bruker> hentBrukereForEnhet(String enhetId, String sortOrder, Filtervalg filtervalg) {
        String queryString = "enhet_id: " + enhetId;
        return hentBrukere(queryString, sortOrder, filtervalg, SolrUtils.brukerErNyComparator());
    }

    public List<Bruker> hentBrukereForVeileder(String veilederIdent, String enhetId, String sortOrder, Filtervalg filtervalg) {
        String queryString = "veileder_id: " + veilederIdent + " AND enhet_id: " + enhetId;
        return hentBrukere(queryString, sortOrder, filtervalg, null);
    }

    public List<Bruker> hentBrukere(String queryString, String sortOrder, Filtervalg filtervalg, Comparator<Bruker> erNyComparator) {
        List<Bruker> brukere = new ArrayList<>();
        try {
            QueryResponse response = solrClient.query(SolrUtils.buildSolrQuery(queryString, filtervalg));
            SolrUtils.checkSolrResponseCode(response.getStatus());
            SolrDocumentList results = response.getResults();
            logger.debug(results.toString());
            brukere = results.stream().map(Bruker::of).collect(toList());
        } catch (SolrServerException | IOException e) {
            logger.error("Spørring mot indeks feilet: ", e.getMessage(), e);
        }
        return SolrUtils.sortBrukere(brukere, sortOrder, erNyComparator);
    }

    public FacetResults hentPortefoljestorrelser(String enhetId) {

        String facetFieldString = "veileder_id";

        SolrQuery solrQuery = SolrUtils.buildSolrFacetQuery("enhet_id: " + enhetId, facetFieldString);

        QueryResponse response = new QueryResponse();
        try {
            response = solrClient.query(solrQuery);
            logger.debug(response.toString());
        } catch (SolrServerException | IOException e) {
            logger.error("Spørring mot indeks feilet", e.getMessage(), e);
        }

        FacetField facetField = response.getFacetField(facetFieldString);

        return SolrUtils.mapFacetResults(facetField);
    }

    public void indekserBrukerMedVeileder(String personId) {
        logger.info("Legger bruker med personId % til i indeks ", personId);
        List<Map<String, Object>> rader = brukerRepository.retrieveBrukerSomHarVeileder(personId);
        List<SolrInputDocument> dokumenter = rader.stream().map(DbUtils::mapRadTilDokument).collect(Collectors.toList());
        addDocuments(dokumenter);
        commit();
        logger.info("Bruker med personId {} lagt til i indeksen", personId);
    }

    public Try<UpdateResponse> commit() {
        return Try.of(() -> solrClient.commit())
                .onFailure(e -> logger.error("Kunne ikke gjennomføre commit ved indeksering!", e));
    }

    public List<SolrInputDocument> addDocuments(List<SolrInputDocument> dokumenter) {
        // javaslang.collection-API brukes her pga sliding-metoden
        javaslang.collection.List.ofAll(dokumenter)
                .sliding(10000, 10000)
                .forEach(docs -> {
                    try {
                        solrClient.add(docs.toJavaList());
                        logger.info(format("Legger til %d dokumenter i indeksen", docs.length()));
                    } catch (SolrServerException | IOException e) {
                        logger.error("Kunne ikke legge til dokumenter.", e.getMessage(), e);
                    }
                });
        return dokumenter;
    }

    private void deleteAllDocuments() {
        try {
            UpdateResponse response = solrClient.deleteByQuery("*:*");
            SolrUtils.checkSolrResponseCode(response.getStatus());
        } catch (SolrServerException | IOException e) {
            logger.error("Kunne ikke slette dokumenter.", e.getMessage(), e);
        } catch (SolrUpdateResponseCodeException e) {
            logger.error(e.getMessage());
        }
    }

    private void logFerdig(LocalDateTime t0, int antall, String indekseringstype) {
        Duration duration = Duration.between(t0, LocalDateTime.now());
        long hours = duration.toHours();
        long minutes = duration.toMinutes();
        long seconds = duration.getSeconds();
        String logString = format("%s fullført! | Tid brukt(hh:mm:ss): %02d:%02d:%02d | Dokumenter oppdatert: %d", indekseringstype, hours, minutes, seconds, antall);
        logger.info(logString);
    }

}
