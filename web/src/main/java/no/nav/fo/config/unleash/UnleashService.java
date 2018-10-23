package no.nav.fo.config.unleash;

import lombok.extern.slf4j.Slf4j;
import no.finn.unleash.DefaultUnleash;
import no.finn.unleash.UnleashContext;
import no.finn.unleash.repository.FeatureToggleRepository;
import no.finn.unleash.repository.FeatureToggleResponse;
import no.finn.unleash.repository.HttpToggleFetcher;
import no.finn.unleash.repository.ToggleBackupHandlerFile;
import no.finn.unleash.strategy.Strategy;
import no.finn.unleash.util.UnleashConfig;
import no.finn.unleash.util.UnleashScheduledExecutor;
import no.finn.unleash.util.UnleashScheduledExecutorImpl;
import no.nav.fo.util.TokenUtils;
import no.nav.sbl.dialogarena.types.Pingable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.vavr.control.Try;

import static java.util.Optional.ofNullable;
import static no.finn.unleash.repository.FeatureToggleResponse.Status.CHANGED;
import static no.nav.brukerdialog.security.context.SubjectHandler.getSubjectHandler;

@Slf4j
public class UnleashService implements Pingable {

    private static final UnleashScheduledExecutor UNLEASH_SCHEDULED_EXECUTOR = new UnleashScheduledExecutorImpl();

    private final DefaultUnleash defaultUnleash;
    private final FeatureToggleRepository featureToggleRepository;
    private final Ping.PingMetadata helsesjekkMetadata;
    private final HttpToggleFetcher pingToggleFetcher;

    public UnleashService(UnleashServiceConfig unleashServiceConfig, Strategy... strategies) {
        this(unleashServiceConfig, Arrays.asList(strategies));
    }

    public UnleashService(UnleashServiceConfig unleashServiceConfig, List<Strategy> strategies) {
        String unleashAPI = unleashServiceConfig.unleashApiUrl;
        UnleashConfig unleashConfig = UnleashConfig.builder()
                .appName(unleashServiceConfig.applicationName)
                .unleashAPI(unleashAPI)
                .build();
        UnleashScheduledExecutor unleashScheduledExecutor = ofNullable(unleashServiceConfig.unleashScheduledExecutor).orElse(UNLEASH_SCHEDULED_EXECUTOR);

        HttpToggleFetcher realToggleFetcher = new HttpToggleFetcher(unleashConfig);
        // Create own instance of HttpToggleFetcher for ping because of its internal cache
        // By reusing HttpToggleFetcher, the scheduled fetching will not observe changes
        this.pingToggleFetcher = new HttpToggleFetcher(unleashConfig);

        this.featureToggleRepository = new FeatureToggleRepository(
                unleashConfig,
                unleashScheduledExecutor,
                realToggleFetcher,
                new ToggleBackupHandlerFile(unleashConfig)
        );
        this.helsesjekkMetadata = new Ping.PingMetadata("unleash", unleashAPI, "sjekker at feature-toggles kan hentes fra unleash server", false);
        this.defaultUnleash = new DefaultUnleash(unleashConfig, featureToggleRepository, addDefaultStrategies(strategies));
    }

    private Strategy[] addDefaultStrategies(List<Strategy> strategies) {
        List<Strategy> list = new ArrayList<>(strategies);
        list.addAll(Arrays.asList(
                new IsNotProdStrategy(),
                new ByEnvironmentStrategy()
        ));
        return list.toArray(new Strategy[0]);
    }

    public boolean isEnabled(String toggleName) {
        return isEnabled(toggleName, resolveUnleashContextFromSubject());
    }

    public boolean isEnabled(String toggleName, UnleashContext unleashContext) {
        return defaultUnleash.isEnabled(toggleName, unleashContext);
    }

    public static UnleashContext resolveUnleashContextFromSubject() {
        String userId = Try.of(() -> getSubjectHandler().getUid()).getOrNull();
        String token = Try.of(() -> TokenUtils.getTokenBody(getSubjectHandler().getSubject())).getOrNull();
        return UnleashContext.builder()
                .userId(userId)
                .sessionId(token)
                .build();
    }

    @Override
    public Ping ping() {
        try {
            FeatureToggleResponse featureToggleResponse = this.pingToggleFetcher.fetchToggles();
            FeatureToggleResponse.Status status = featureToggleResponse.getStatus();
            if (status == CHANGED || status == FeatureToggleResponse.Status.NOT_CHANGED) {
                return Ping.lyktes(helsesjekkMetadata);
            } else {
                return Ping.feilet(helsesjekkMetadata, status.toString());
            }
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            return Ping.feilet(helsesjekkMetadata, e);
        }
    }

}
