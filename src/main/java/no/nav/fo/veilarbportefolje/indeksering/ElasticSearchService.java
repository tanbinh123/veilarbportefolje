package no.nav.fo.veilarbportefolje.indeksering;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import no.nav.fo.veilarbportefolje.aktivitet.AktivitetDAO;
import no.nav.fo.veilarbportefolje.database.BrukerRepository;
import no.nav.fo.veilarbportefolje.domene.*;
import no.nav.fo.veilarbportefolje.util.Utils;
import no.nav.json.JsonUtils;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

import javax.inject.Inject;
import java.time.Instant;
import java.util.*;

import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static no.nav.fo.veilarbportefolje.config.DatabaseConfig.ES_DELTAINDEKSERING;
import static no.nav.fo.veilarbportefolje.config.DatabaseConfig.ES_TOTALINDEKSERING;
import static no.nav.fo.veilarbportefolje.indeksering.ElasticSearchUtils.finnBruker;
import static no.nav.fo.veilarbportefolje.indeksering.IndekseringConfig.*;
import static no.nav.fo.veilarbportefolje.util.AktivitetUtils.filtrerBrukertiltak;
import static no.nav.fo.veilarbportefolje.util.UnderOppfolgingRegler.erUnderOppfolging;
import static org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions.Type.ADD;
import static org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions.Type.REMOVE;
import static org.elasticsearch.client.RequestOptions.DEFAULT;

@Slf4j
public class ElasticSearchService implements IndekseringService {

    private RestHighLevelClient client;

    private AktivitetDAO aktivitetDAO;

    private BrukerRepository brukerRepository;

    private LockingTaskExecutor shedlock;

    static String mappingJson = "{\n" +
            "  \"properties\": {\n" +
            "    \"veileder_id\": {\n" +
            "      \"type\": \"keyword\"\n" +
            "    },\n" +
            "    \"enhet_id\": {\n" +
            "      \"type\": \"keyword\"\n" +
            "    },\n" +
            "    \"person_id\": {\n" +
            "      \"type\": \"keyword\"\n" +
            "    }\n" +
            "  }\n" +
            "}";

    @Inject
    public ElasticSearchService(RestHighLevelClient client, AktivitetDAO aktivitetDAO, BrukerRepository brukerRepository, LockingTaskExecutor shedlock) {
        this.client = client;
        this.aktivitetDAO = aktivitetDAO;
        this.brukerRepository = brukerRepository;
        this.shedlock = shedlock;
    }

    @Override
    public void hovedindeksering() {
        shedlock.executeWithLock(() -> {
                    try {
                        startIndeksering();
                        HovedIndekseringHelsesjekk.setIndekseringVellykket();
                    } catch (Exception e) {
                        log.error("Hovedindeksering: indeksering feilet {}", e.getMessage());
                        HovedIndekseringHelsesjekk.setIndekseringFeilet(e);
                    }
                },
                new LockConfiguration(ES_TOTALINDEKSERING, Instant.now().plusSeconds(60 * 60 * 3))
        );
    }

    @SneakyThrows
    private void startIndeksering() {
        log.info("Hovedindeksering: Starter hovedindeksering i Elasticsearch");
        long t0 = System.currentTimeMillis();

        String nyIndeks = opprettNyIndeks();
        log.info("Hovedindeksering: Opprettet ny index {}", nyIndeks);


        List<BrukerDTO> brukere = brukerRepository.hentAlleBrukereUnderOppfolging();
        log.info("Hovedindeksering: Hentet {} oppfølgingsbrukere fra databasen", brukere.size());

        log.info("Hovedindeksering: Batcher opp uthenting av aktiviteter og tiltak samt skriveoperasjon til indeks (BATCH_SIZE={})", BATCH_SIZE);
        Utils.splittOppListe(brukere, BATCH_SIZE).forEach(brukerBatch -> {
            leggTilAktiviteter(brukerBatch);
            leggTilTiltak(brukerBatch);
            skrivTilIndeks(nyIndeks, brukerBatch);
        });

        Optional<String> gammelIndeks = hentGammeltIndeksNavn();
        if (gammelIndeks.isPresent()) {
            log.info("Hovedindeksering: Peker alias mot ny indeks og sletter den gamle");
            flyttAliasTilNyIndeks(gammelIndeks.get(), nyIndeks);
            slettGammelIndeks(gammelIndeks.get());
        } else {
            log.info("Hovedindeksering: Lager alias til ny indeks");
            leggTilAliasTilIndeks(nyIndeks);
        }

        long t1 = System.currentTimeMillis();
        long time = t1 - t0;

        log.info("Hovedindeksering: Hovedindeksering for {} brukere fullførte på {}ms", brukere.size(), time);
    }

    @Override
    public void deltaindeksering() {
        shedlock.executeWithLock(() -> {

            log.info("Deltaindeksering: Starter deltaindeksering i Elasticsearch");
            List<BrukerDTO> brukere = brukerRepository.hentOppdaterteBrukereUnderOppfolging();

            log.info("Deltaindeksering: Hentet ut {} oppdaterte brukere i Elasticsearch", brukere.size());

            Utils.splittOppListe(brukere, BATCH_SIZE).forEach(brukerBatch -> {
                leggTilAktiviteter(brukere);
                leggTilTiltak(brukere);
                skrivTilIndeks(ALIAS, brukere);
            });

            log.info("Deltaindeksering: Deltaindeksering for {} brukere er utført", brukere.size());

        }, new LockConfiguration(ES_DELTAINDEKSERING, Instant.now().plusSeconds(50)));
    }

    @Override
    @SneakyThrows
    public void slettBruker(String fnr) {

        DeleteByQueryRequest deleteQuery = new DeleteByQueryRequest()
                .setQuery(new TermQueryBuilder("fnr", fnr));

        BulkByScrollResponse response = client.deleteByQuery(deleteQuery, DEFAULT);
        if (response.getDeleted() != 1) {
            log.error("Feil ved sletting av bruker i indeks {}", response.toString());
        }
    }


    @Override
    public void indekserAsynkront(AktoerId aktoerId) {
        runAsync(() -> {
            BrukerDTO bruker = brukerRepository.hentBruker(aktoerId);

            if (erUnderOppfolging(bruker)) {
                leggTilAktiviteter(bruker);
                leggTilTiltak(bruker);
                skrivTilIndeks(ALIAS, bruker);
            } else {
                slettBruker(bruker.fnr);
            }

        });
    }

    @Override
    public void indekserBrukere(List<PersonId> personIds) {
        Utils.splittOppListe(personIds, BATCH_SIZE).forEach(batch -> {
            List<BrukerDTO> brukere = brukerRepository.hentBrukere(batch);
            leggTilAktiviteter(brukere);
            leggTilTiltak(brukere);
            skrivTilIndeks(ALIAS, brukere);
        });
    }

    @SneakyThrows
    private Optional<String> hentGammeltIndeksNavn() {
        GetAliasesRequest getAliasRequest = new GetAliasesRequest(ALIAS);
        GetAliasesResponse aliasResponse = client.indices().getAlias(getAliasRequest, DEFAULT);
        return aliasResponse.getAliases().keySet().stream().findFirst();
    }

    @SneakyThrows
    private void leggTilAliasTilIndeks(String indeks) {
        AliasActions addAliasAction = new AliasActions(ADD)
                .index(indeks)
                .alias(ALIAS);

        IndicesAliasesRequest request = new IndicesAliasesRequest().addAliasAction(addAliasAction);

        AcknowledgedResponse addAliasResponse = client.indices().updateAliases(request, DEFAULT);
        if (!addAliasResponse.isAcknowledged()) {
            log.error("Kunne ikke legge til alias {}", ALIAS);
        }
    }

    @SneakyThrows
    private void flyttAliasTilNyIndeks(String gammelIndeks, String nyIndeks) {
        AliasActions addAliasAction = new AliasActions(ADD)
                .index(nyIndeks)
                .alias(ALIAS);

        AliasActions removeAliasAction = new AliasActions(REMOVE)
                .index(gammelIndeks)
                .alias(ALIAS);

        IndicesAliasesRequest request = new IndicesAliasesRequest()
                .addAliasAction(removeAliasAction)
                .addAliasAction(addAliasAction);

        AcknowledgedResponse response = client.indices().updateAliases(request, DEFAULT);

        if (!response.isAcknowledged()) {
            log.error("Kunne ikke oppdatere alias {}", ALIAS);
        }
    }

    @SneakyThrows
    private void slettGammelIndeks(String gammelIndeks) {
        AcknowledgedResponse response = client
                .indices()
                .delete(new DeleteIndexRequest(gammelIndeks), DEFAULT);

        if (!response.isAcknowledged()) {
            log.warn("Kunne ikke slette gammel indeks {}", gammelIndeks);
        }
    }

    @SneakyThrows
    private void skrivTilIndeks(String indeksNavn, List<BrukerDTO> oppfolgingsBrukere) {
        BulkRequest bulk = new BulkRequest();
        oppfolgingsBrukere.stream()
                .map(JsonUtils::toJson)
                .map(json -> new IndexRequest(indeksNavn, "_doc").source(json, XContentType.JSON))
                .forEach(bulk::add);

        BulkResponse response = client.bulk(bulk, DEFAULT);
        if (response.hasFailures()) {
            throw new RuntimeException(response.buildFailureMessage());
        }
    }

    private void skrivTilIndeks(String indeksNavn, BrukerDTO brukerDTO) {
        skrivTilIndeks(indeksNavn, Collections.singletonList(brukerDTO));
    }

    @SneakyThrows
    private String opprettNyIndeks() {
        String indexName = ElasticSearchUtils.createIndexName(ALIAS);
        CreateIndexRequest request = new CreateIndexRequest(indexName)
                .mapping("_doc", mappingJson, XContentType.JSON);
        client.indices().create(request, DEFAULT);
        return indexName;
    }

    private void validateBatchSize(List<BrukerDTO> brukere) {
        if (brukere.size() > BATCH_SIZE_LIMIT) {
            throw new IllegalStateException(format("Kan ikke prossessere flere enn %s brukere av gangen pga begrensninger i oracle db", BATCH_SIZE_LIMIT));
        }
    }

    private void leggTilTiltak(List<BrukerDTO> brukere) {

        validateBatchSize(brukere);

        List<Fnr> fodselsnummere = brukere.stream()
                .map(BrukerDTO::getFnr)
                .map(Fnr::of)
                .collect(toList());

        Map<Fnr, Set<Brukertiltak>> alleTiltakForBrukere = filtrerBrukertiltak(aktivitetDAO.hentBrukertiltak(fodselsnummere));

        alleTiltakForBrukere.forEach((fnr, brukerMedTiltak) -> {
            Set<String> tiltak = brukerMedTiltak.stream()
                    .map(Brukertiltak::getTiltak)
                    .collect(toSet());

            BrukerDTO bruker = finnBruker(brukere, fnr);
            bruker.setTiltak(tiltak);
        });
    }

    private void leggTilTiltak(BrukerDTO bruker) {
        leggTilAktiviteter(Collections.singletonList(bruker));
    }

    private void leggTilAktiviteter(List<BrukerDTO> brukere) {

        validateBatchSize(brukere);

        List<PersonId> personIder = brukere.stream()
                .map(BrukerDTO::getPerson_id)
                .map(PersonId::of)
                .collect(toList());

        Map<PersonId, Set<AktivitetStatus>> alleAktiviteterForBrukere = aktivitetDAO.getAktivitetstatusForBrukere(personIder);

        alleAktiviteterForBrukere.forEach((personId, statuserForBruker) -> {

            BrukerDTO bruker = finnBruker(brukere, personId);

            statuserForBruker.forEach(status -> ElasticSearchUtils.leggTilUtlopsDato(bruker, status));

            Set<String> aktiviteterSomErAktive = statuserForBruker.stream()
                    .filter(AktivitetStatus::isAktiv)
                    .map(AktivitetStatus::getAktivitetType)
                    .collect(toSet());

            bruker.setAktiviteter(aktiviteterSomErAktive);
        });
    }

    private void leggTilAktiviteter(BrukerDTO bruker) {
        leggTilAktiviteter(Collections.singletonList(bruker));
    }


    @Override
    public BrukereMedAntall hentBrukere(String enhetId, Optional<String> veilederIdent, String sortOrder, String sortField, Filtervalg filtervalg, Integer fra, Integer antall) {
        throw new IllegalStateException();
    }

    @Override
    public BrukereMedAntall hentBrukere(String enhetId, Optional<String> veilederIdent, String sortOrder, String sortField, Filtervalg filtervalg) {
        throw new IllegalStateException();
    }

    @Override
    public StatusTall hentStatusTallForPortefolje(String enhet) {
        throw new IllegalStateException();
    }

    @Override
    public FacetResults hentPortefoljestorrelser(String enhetId) {
        throw new IllegalStateException();
    }

    @Override
    public StatusTall hentStatusTallForVeileder(String enhet, String veilederIdent) {
        throw new IllegalStateException();
    }

    @Override
    public List<Bruker> hentBrukereMedArbeidsliste(VeilederId veilederId, String enhet) {
        throw new IllegalStateException();
    }


}
