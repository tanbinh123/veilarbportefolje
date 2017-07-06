package no.nav.fo.config;

import no.nav.apiapp.ApiApplication;
import no.nav.fo.service.OppdaterBrukerdataFletter;
import no.nav.fo.service.PepClient;
import no.nav.fo.service.PepClientMock;
import no.nav.sbl.dialogarena.common.abac.pep.context.AbacContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

import static no.nav.apiapp.ApiApplication.Sone.FSS;

@EnableAspectJAutoProxy
@EnableScheduling
@Configuration
@Import({
        DatabaseConfig.class,
        VirksomhetEnhetEndpointConfigMock.class,
        LocalServiceConfig.class,
        SolrConfig.class,
        AktoerEndpointConfig.class,
        ArenafilConfig.class,
        MetricsConfig.class,
        AktoerEndpointConfig.class,
        AbacContext.class,
        CacheConfig.class
})
public class LocalApplicationConfig implements ApiApplication{

    @Bean(name = "transactionManager")
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    public OppdaterBrukerdataFletter tilordneVeilederFletter() {
        return new OppdaterBrukerdataFletter();
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer placeholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean
    public PepClient pepClient() {
        return new PepClientMock();
    }

    @Override
    public Sone getSone() {
        return FSS;
    }
}
