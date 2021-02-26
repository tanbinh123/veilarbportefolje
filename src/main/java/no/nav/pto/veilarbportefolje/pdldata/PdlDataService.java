package no.nav.pto.veilarbportefolje.pdldata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import no.nav.common.client.pdl.PdlClient;
import no.nav.common.client.utils.graphql.GraphqlRequest;
import no.nav.common.client.utils.graphql.GraphqlResponse;
import no.nav.common.featuretoggle.UnleashService;
import no.nav.common.json.JsonUtils;
import no.nav.common.rest.client.RestClient;
import no.nav.common.rest.client.RestUtils;
import no.nav.common.types.identer.AktorId;
import no.nav.common.types.identer.Fnr;
import no.nav.common.utils.FileUtils;
import no.nav.pto.veilarbportefolje.auth.AuthUtils;
import no.nav.pto.veilarbportefolje.config.FeatureToggle;
import no.nav.pto.veilarbportefolje.database.BrukerRepository;
import no.nav.pto.veilarbportefolje.domene.AktorClient;
import no.nav.pto.veilarbportefolje.elastic.domene.OppfolgingsBruker;
import no.nav.pto.veilarbportefolje.util.DateUtils;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

import static no.nav.common.utils.UrlUtils.joinPaths;
import static org.springframework.http.HttpHeaders.ACCEPT;
import static org.springframework.http.HttpHeaders.AUTHORIZATION;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@Service
@Slf4j
public class PdlDataService {
    private final PdlRepository pdlRepository;
    private final PdlClient pdlClient;
    private final BrukerRepository brukerRepository;
    private final String hentPersonQuery;
    private final UnleashService unleashService;
    private final OkHttpClient client;
    private final AktorClient aktorClient;

    @Autowired
    public PdlDataService(PdlRepository pdlRepository, PdlClient pdlClient, BrukerRepository brukerRepository, UnleashService unleashService, AktorClient aktorClient) {
        this.brukerRepository = brukerRepository;
        this.pdlRepository = pdlRepository;
        this.pdlClient = pdlClient;
        this.unleashService = unleashService;
        this.aktorClient = aktorClient;
        this.hentPersonQuery = FileUtils.getResourceFileAsString("graphql/hentPersonFodselsdag.gql");

        this.client = RestClient.baseClient();
    }

    public void lastInnPdlData(AktorId aktorId) {
        String fodselsdag;
        if (erPdlPa(unleashService)) {
            fodselsdag = hentFodseldagFraPdl(aktorId);
        } else {
            fodselsdag = hentFodseldagFraTps(aktorId);
        }
        pdlRepository.upsert(aktorId, DateUtils.getLocalDateFromSimpleISODate(fodselsdag));
    }

    public void slettPdlData(AktorId aktorId) {
        pdlRepository.slettPdlData(aktorId);
    }

    public void lastInnDataFraDbLinkTilPdlDataTabell() {
        List<OppfolgingsBruker> brukere = brukerRepository.hentAlleBrukereUnderOppfolging();
        log.info("lastInnDataFraDbLinkTilPdlDataTabell: Hentet {} oppfølgingsbrukere fra databasen", brukere.size());
        pdlRepository.saveBatch(brukere);
        log.info("lastInnDataFraDbLinkTilPdlDataTabell: fullført");
    }

    @SneakyThrows
    private String hentFodseldagFraPdl(AktorId aktorId) {
        GraphqlRequest<PdlPersonVariables.HentFodselsdag> request = new GraphqlRequest<>(hentPersonQuery, new PdlPersonVariables.HentFodselsdag(aktorId.get()));
        PdlFodselsRespons respons = pdlClient.request(request, PdlFodselsRespons.class);
        if(hasErrors(respons)){
            throw new RuntimeException();
        }

        return respons.getData()
                .getHentPerson()
                .getFoedsel()
                .stream()
                .findFirst()
                .map(fodselsdag-> fodselsdag.getFoedselsdato())
                .orElseThrow();
    }


    @SneakyThrows
    private String hentFodseldagFraTps(AktorId aktorId) {
        return null;
    }

    private boolean erPdlPa(UnleashService unleashService) {
        return unleashService.isEnabled(FeatureToggle.PDL);
    }

    public static <T> boolean hasErrors(GraphqlResponse<T> response){
        if(response.getErrors() == null){
            return false;
        }
        return !response.getErrors().isEmpty();
    }
}
