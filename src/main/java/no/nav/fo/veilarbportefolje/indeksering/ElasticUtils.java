package no.nav.fo.veilarbportefolje.indeksering;

import lombok.extern.slf4j.Slf4j;
import no.nav.fo.veilarbportefolje.indeksering.domene.CountResponse;
import no.nav.sbl.rest.RestUtils;
import no.nav.sbl.util.EnvironmentUtils;

import java.util.Base64;

import static no.nav.fo.veilarbportefolje.indeksering.ElasticConfig.VEILARBELASTIC_PASSWORD;
import static no.nav.fo.veilarbportefolje.indeksering.ElasticConfig.VEILARBELASTIC_USERNAME;
import static no.nav.sbl.util.EnvironmentUtils.resolveHostName;

@Slf4j
public class ElasticUtils {

    public static final String NAIS_LOADBALANCED_HOSTNAME = "tpa-veilarbelastic-elasticsearch.nais.preprod.local";
    public static final String NAIS_INTERNAL_CLUSTER_HOSTNAME = "tpa-veilarbelastic-elasticsearch.tpa.svc.nais.local";

    public static long getCount() {
        String url = ElasticUtils.getAbsoluteUrl() + "_doc/_count";

        return RestUtils.withClient(client ->
                client
                        .target(url)
                        .request()
                        .header("Authorization", ElasticUtils.getAuthHeaderValue())
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
        return "Basic "  + Base64.getEncoder().encodeToString(auth.getBytes());
    }

    static String getAlias() {
        return String.format("brukerindeks_%s", EnvironmentUtils.requireEnvironmentName());
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
        log.info("Kjører på hostname {}", hostname);
        return hostname.contains("devillo.no");
    }
}