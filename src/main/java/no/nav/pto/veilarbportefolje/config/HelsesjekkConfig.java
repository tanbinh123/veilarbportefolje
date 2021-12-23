package no.nav.pto.veilarbportefolje.config;

import no.nav.common.abac.Pep;
import no.nav.common.health.selftest.SelfTestCheck;
import no.nav.common.health.selftest.SelfTestChecks;
import no.nav.common.health.selftest.SelfTestMeterBinder;
import no.nav.pto.veilarbportefolje.domene.AktorClient;
import no.nav.pto.veilarbportefolje.elastic.ElasticHealthCheck;
import no.nav.pto.veilarbportefolje.service.UnleashService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

import static no.nav.pto.veilarbportefolje.config.DbConfigOracle.dbPinger;
import static no.nav.pto.veilarbportefolje.elastic.ElasticHealthCheck.FORVENTET_MINIMUM_ANTALL_DOKUMENTER;

@Configuration
@Profile("production")
public class HelsesjekkConfig {

    @Bean
    public SelfTestChecks selfTestChecks(AktorClient aktorClient,
                                         Pep veilarbPep,
                                         JdbcTemplate jdbcTemplate,
                                         UnleashService unleashService,
                                         ElasticHealthCheck elasticHealthCheck) {
        List<SelfTestCheck> asyncSelftester = List.of(
                new SelfTestCheck(String.format("Sjekker at antall dokumenter > %s", FORVENTET_MINIMUM_ANTALL_DOKUMENTER), false, elasticHealthCheck),
                new SelfTestCheck("Database for portefolje", true, () -> dbPinger(jdbcTemplate)),
                new SelfTestCheck("Aktorregister", true, aktorClient),
                new SelfTestCheck("ABAC", true, veilarbPep.getAbacClient()),
                new SelfTestCheck("Sjekker at feature-toggles kan hentes fra Unleash", false, unleashService)
        );
        return new SelfTestChecks(asyncSelftester);
    }

    @Bean
    public SelfTestMeterBinder selfTestMeterBinder(SelfTestChecks selfTestChecks) {
        return new SelfTestMeterBinder(selfTestChecks);
    }
}
