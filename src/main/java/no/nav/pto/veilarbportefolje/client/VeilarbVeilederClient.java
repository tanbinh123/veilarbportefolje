package no.nav.pto.veilarbportefolje.client;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import no.nav.common.featuretoggle.UnleashService;
import no.nav.common.rest.client.RestClient;
import no.nav.common.rest.client.RestUtils;
import no.nav.common.types.identer.EnhetId;
import no.nav.pto.veilarbportefolje.config.EnvironmentProperties;
import no.nav.pto.veilarbportefolje.database.BrukerRepositoryV2;
import no.nav.pto.veilarbportefolje.oppfolging.OppfolgingRepositoryV2;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static no.nav.common.client.utils.CacheUtils.tryCacheFirst;
import static no.nav.pto.veilarbportefolje.client.RestClientUtils.authHeaderMedSystemBruker;
import static no.nav.pto.veilarbportefolje.config.FeatureToggle.erPostgresPa;
import static org.springframework.http.HttpHeaders.AUTHORIZATION;

@Slf4j
@Component
public class VeilarbVeilederClient {

    private final String url;
    private final OkHttpClient client;
    private final Cache<EnhetId, List<String>> hentVeilederePaaEnhetCache;
    private final OppfolgingRepositoryV2 oppfolgingRepositoryV2;
    private final BrukerRepositoryV2 brukerRepositoryV2;
    private final UnleashService unleashService;

    @Autowired
    public VeilarbVeilederClient(EnvironmentProperties environmentProperties, OppfolgingRepositoryV2 oppfolgingRepositoryV2, BrukerRepositoryV2 brukerRepositoryV2, UnleashService unleashService) {
        this.url = environmentProperties.getVeilarbVeilederUrl();
        this.oppfolgingRepositoryV2 = oppfolgingRepositoryV2;
        this.brukerRepositoryV2 = brukerRepositoryV2;
        this.unleashService = unleashService;
        this.client = RestClient.baseClient();
        hentVeilederePaaEnhetCache = Caffeine.newBuilder()
                .expireAfterWrite(10, TimeUnit.HOURS)
                .maximumSize(600)
                .build();
    }

    public List<String> hentVeilederePaaEnhet(EnhetId enhet) {
        return tryCacheFirst(hentVeilederePaaEnhetCache, enhet,
                () -> oppdaterUfordelteBrukerePaEnhet(enhet));
    }

    public void oppdaterUfordelteBrukerePaEnhetCached(EnhetId enhet) {
        tryCacheFirst(hentVeilederePaaEnhetCache, enhet,
                () -> oppdaterUfordelteBrukerePaEnhet(enhet));
    }

    @SneakyThrows
    private List<String> oppdaterUfordelteBrukerePaEnhet(EnhetId enhet) {
        final List<String> veilederePaEnhet = hentVeilederePaaEnhetQuery(enhet);
        if (!erPostgresPa(unleashService)) {
            return veilederePaEnhet;
        }

        brukerRepositoryV2.getUfordeltBrukerDTOerPaEnhet(enhet).ifPresent(brukerePaEnhet ->
                brukerePaEnhet.stream()
                        .filter(bruker -> bruker.getVeileder() != null && bruker.getAktorId() != null)
                        .forEach(bruker ->
                                oppfolgingRepositoryV2.settUfordeltStatus(bruker.getAktorId(), veilederePaEnhet.contains(bruker.getVeileder()))
                        )
        );

        return veilederePaEnhet;
    }

    @SneakyThrows
    private List<String> hentVeilederePaaEnhetQuery(EnhetId enhet) {
        String path = format("/enhet/%s/identer", enhet);
        Request request = new Request.Builder()
                .header(AUTHORIZATION, authHeaderMedSystemBruker())
                .url(url + path)
                .build();

        try (Response response = client.newCall(request).execute()) {
            RestUtils.throwIfNotSuccessful(response);
            return RestUtils.parseJsonResponseArrayOrThrow(response, String.class);
        }
    }
}
