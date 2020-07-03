package no.nav.pto.veilarbportefolje.elastic;

import lombok.extern.slf4j.Slf4j;
import no.nav.pto.veilarbportefolje.domene.Fnr;
import no.nav.pto.veilarbportefolje.elastic.domene.CountResponse;
import no.nav.pto.veilarbportefolje.elastic.domene.ElasticClientConfig;
import no.nav.sbl.rest.RestUtils;
import no.nav.sbl.util.EnvironmentUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

import static no.nav.pto.veilarbportefolje.elastic.ElasticConfig.VEILARBELASTIC_PASSWORD;
import static no.nav.pto.veilarbportefolje.elastic.ElasticConfig.VEILARBELASTIC_USERNAME;
import static no.nav.sbl.util.EnvironmentUtils.resolveHostName;

@Slf4j
public class ElasticUtils {

    public static final String NAIS_LOADBALANCED_HOSTNAME = "tpa-veilarbelastic-elasticsearch.nais.preprod.local";
    public static final String NAIS_INTERNAL_CLUSTER_HOSTNAME = "tpa-veilarbelastic-elasticsearch.tpa.svc.nais.local";

    private static int SOCKET_TIMEOUT = 120_000;
    private static int CONNECT_TIMEOUT = 60_000;

    public static UpdateRequest creatUpdateRequest(String indexName, Fnr fnr, String json) {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(indexName);
        updateRequest.type("_doc");
        updateRequest.id(fnr.getFnr());
        updateRequest.docAsUpsert(true);
        updateRequest.doc(json, XContentType.JSON);
        return updateRequest;
    }

    public static RestHighLevelClient createClient(ElasticClientConfig config) {
        HttpHost httpHost = new HttpHost(
                config.getHostname(),
                config.getPort(),
                config.getScheme()
        );

        return new RestHighLevelClient(RestClient.builder(httpHost)
                .setHttpClientConfigCallback(getHttpClientConfigCallback(config))
                .setMaxRetryTimeoutMillis(SOCKET_TIMEOUT)
                .setRequestConfigCallback(
                        requestConfig -> {
                            requestConfig.setConnectTimeout(CONNECT_TIMEOUT);
                            requestConfig.setSocketTimeout(SOCKET_TIMEOUT);
                            requestConfig.setConnectionRequestTimeout(0); // http://www.github.com/elastic/elasticsearch/issues/24069
                            return requestConfig;
                        }
                ));
    }

    private static RestClientBuilder.HttpClientConfigCallback getHttpClientConfigCallback(ElasticClientConfig config) {
        if (config.isDisableSecurity()) {
            return httpClientBuilder -> httpClientBuilder;
        }

        return new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setDefaultCredentialsProvider(createCredentialsProvider());
            }

            private CredentialsProvider createCredentialsProvider() {
                UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(
                        config.getUsername(),
                        config.getPassword()
                );

                BasicCredentialsProvider provider = new BasicCredentialsProvider();
                provider.setCredentials(AuthScope.ANY, credentials);
                return provider;
            }
        };
    }


    public static String createIndexName(String alias) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
        String timestamp = LocalDateTime.now().format(formatter);
        return String.format("%s_%s", alias, timestamp);
    }


    public static long getCount() {
        String url = ElasticUtils.getAbsoluteUrl() + "_doc/_count";

        return RestUtils.withClient(client ->
                client
                        .target(url)
                        .request()
                        .header("Authorization", getAuthHeaderValue())
                        .get(CountResponse.class)
                        .getCount()
        );
    }

    static String getAbsoluteUrl() {
        return String.format(
                "%s://%s:%s/%s/",
                getElasticScheme(),
                getElasticHostname(),
                getElasticPort(),
                getAlias()
        );
    }

    static String getAuthHeaderValue() {
        String auth = VEILARBELASTIC_USERNAME + ":" + VEILARBELASTIC_PASSWORD;
        return "Basic " + Base64.getEncoder().encodeToString(auth.getBytes());
    }

    public static String getAlias() {
        return String.format("brukerindeks_%s", EnvironmentUtils.getNamespace().orElse("localhost"));
    }

    static String getElasticScheme() {
        if (onDevillo()) {
            return "https";
        } else {
            return "http";
        }
    }

    static int getElasticPort() {
        if (onDevillo()) {
            return 443;
        } else {
            return 9200;
        }
    }

    static String getElasticHostname() {
        if (onDevillo()) {
            return NAIS_LOADBALANCED_HOSTNAME;
        } else {
            return NAIS_INTERNAL_CLUSTER_HOSTNAME;
        }
    }

    public static boolean onDevillo() {
        String hostname = resolveHostName();
        return hostname.contains("devillo.no");
    }
}
