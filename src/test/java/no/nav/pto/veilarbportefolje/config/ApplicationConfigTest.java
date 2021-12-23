package no.nav.pto.veilarbportefolje.config;

import no.nav.common.abac.Pep;
import no.nav.common.auth.context.AuthContextHolder;
import no.nav.common.auth.context.AuthContextHolderThreadLocal;
import no.nav.common.job.leader_election.LeaderElectionClient;
import no.nav.common.metrics.MetricsClient;
import no.nav.common.sts.SystemUserTokenProvider;
import no.nav.common.utils.Credentials;
import no.nav.pto.veilarbportefolje.auth.AuthService;
import no.nav.pto.veilarbportefolje.auth.ModiaPep;
import no.nav.pto.veilarbportefolje.client.VeilarbVeilederClient;
import no.nav.pto.veilarbportefolje.domene.AktorClient;
import no.nav.pto.veilarbportefolje.elastic.IndexName;
import no.nav.pto.veilarbportefolje.elastic.domene.ElasticClientConfig;
import no.nav.pto.veilarbportefolje.mock.MetricsClientMock;
import no.nav.pto.veilarbportefolje.service.UnleashService;
import no.nav.pto.veilarbportefolje.util.ElasticTestClient;
import no.nav.pto.veilarbportefolje.util.SingletonPostgresContainer;
import no.nav.pto.veilarbportefolje.util.TestDataClient;
import no.nav.pto.veilarbportefolje.util.TestUtil;
import no.nav.pto.veilarbportefolje.util.VedtakstottePilotRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import javax.sql.DataSource;

import static no.nav.common.utils.IdUtils.generateId;
import static no.nav.pto.veilarbportefolje.elastic.ElasticUtils.createClient;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestConfiguration
@EnableConfigurationProperties({EnvironmentProperties.class})
public class ApplicationConfigTest {

    private static final ElasticsearchContainer ELASTICSEARCH_CONTAINER;
    private static final String ELASTICSEARCH_VERSION = "7.16.1";
    private static final String ELASTICSEARCH_TEST_PASSWORD = "test";
    private static final String ELASTICSEARCH_TEST_USERNAME = "elastic";

    static {
        ELASTICSEARCH_CONTAINER = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:" + ELASTICSEARCH_VERSION);
        ELASTICSEARCH_CONTAINER.withPassword(ELASTICSEARCH_TEST_PASSWORD);
        ELASTICSEARCH_CONTAINER.start();
        System.setProperty("elastic.indexname", generateId());
        System.setProperty("elastic.httphostaddress", ELASTICSEARCH_CONTAINER.getHttpHostAddress());
    }


    @Bean
    public TestDataClient dbTestClient(JdbcTemplate jdbcTemplate, ElasticTestClient elasticTestClient) {
        return new TestDataClient(jdbcTemplate, elasticTestClient);
    }

    @Bean
    public ElasticTestClient elasticTestClient(RestHighLevelClient restHighLevelClient, IndexName indexName) {
        return new ElasticTestClient(restHighLevelClient, indexName);
    }

    @Bean
    public VedtakstottePilotRequest vedtakstottePilotRequest() {
        VedtakstottePilotRequest vedtakstottePilotRequest = mock(VedtakstottePilotRequest.class);
        when(vedtakstottePilotRequest.erVedtakstottePilotPa(any())).thenReturn(true);
        return vedtakstottePilotRequest;
    }

    @Bean
    public Credentials serviceUserCredentials() {
        return new Credentials("username", "password");
    }

    @Bean
    public IndexName indexName() {
        return new IndexName(generateId());
    }

    @Bean
    public AktorClient aktorClient() {
        return mock(AktorClient.class);
    }

    @Bean
    public UnleashService unleashService() {
        final UnleashService mock = mock(UnleashService.class);
        when(mock.isEnabled(anyString())).thenReturn(true);
        when(mock.isEnabled(FeatureToggle.POSTGRES)).thenReturn(false);
        return mock;
    }

    @Bean
    public MetricsClient metricsClient() {
        return new MetricsClientMock();
    }

    @Bean
    @Primary
    public DataSource hsqldbDataSource() {
        return TestUtil.setupInMemoryDatabase();
    }


    @Bean
    @Primary
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    @Primary
    public NamedParameterJdbcTemplate namedParameterJdbcTemplate(DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean
    public PlatformTransactionManager platformTransactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    public RestHighLevelClient restHighLevelClient( ElasticClientConfig elasticTestConfig) {
        return createClient(elasticTestConfig);
    }

    @Bean
    public ElasticClientConfig elasticsearchClientConfig() {
        return ElasticClientConfig.builder()
                .username(ELASTICSEARCH_TEST_USERNAME)
                .password(ELASTICSEARCH_TEST_PASSWORD)
                .hostname(ELASTICSEARCH_CONTAINER.getHost())
                .port(ELASTICSEARCH_CONTAINER.getFirstMappedPort())
                .scheme("http")
                .build();
    }


    @Bean
    public VeilarbVeilederClient veilarbVeilederClient() {
        return mock(VeilarbVeilederClient.class);
    }

    @Bean
    public SystemUserTokenProvider systemUserTokenProvider() {
        return mock(SystemUserTokenProvider.class);
    }

    @Bean
    public LeaderElectionClient leaderElectionClient() {
        return mock(LeaderElectionClient.class);
    }

    @Bean(name = "PostgresJdbc")
    public JdbcTemplate db() {
        return SingletonPostgresContainer.init().createJdbcTemplate();
    }

    @Bean(name = "PostgresJdbcReadOnly")
    public JdbcTemplate dbRead() {
        return SingletonPostgresContainer.init().createJdbcTemplate();
    }

    @Bean
    public AuthContextHolder authContextHolder() {
        return AuthContextHolderThreadLocal.instance();
    }


    @Bean
    public Pep pep() {
        return mock(Pep.class);
    }

    @Bean
    public ModiaPep modiaPep() {
        return mock(ModiaPep.class);
    }
    @Bean
    public AuthService authService(){
        return mock(AuthService.class);
    }
}
