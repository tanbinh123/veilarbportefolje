package no.nav.pto.veilarbportefolje.elastic;

import no.nav.pto.veilarbportefolje.cv.IntegrationTest;
import no.nav.pto.veilarbportefolje.domene.Fnr;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.rest.RestStatus;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static no.nav.common.utils.IdUtils.generateId;
import static org.assertj.core.api.Assertions.assertThat;

public class ElasticServiceV2Test extends IntegrationTest {
    private String indexName;
    private ElasticServiceV2 elasticServiceV2;

    @Before
    public void setUp() {
        indexName = generateId();
        elasticServiceV2 = new ElasticServiceV2(ELASTIC_CLIENT, indexName);
        createIndex(indexName);
    }

    @After
    public void tearDown() {
        deleteIndex(indexName);
    }

    @Test
    public void skal_opprette_dokument_om_dokument_ikke_allerede_finnes() {
        Fnr fnr = Fnr.of("11111111111");
        Optional<UpdateResponse> updateResponse = elasticServiceV2.upsert(fnr, true);
        assertThat(updateResponse.isPresent()).isTrue();
        assertThat(updateResponse.get().status()).isEqualTo(RestStatus.CREATED);
    }

    @Test
    public void skal_oppdatare_dokument_om_dokument_allerede_finnes() {
        Fnr fnr = Fnr.of("11111111111");
        String field = "foo";

        createDocument(indexName, fnr, new JSONObject().put(field, true).toString());
        Optional<UpdateResponse> updateResponse = elasticServiceV2.upsert(fnr, true);
        assertThat(updateResponse.isPresent()).isTrue();
        assertThat(updateResponse.get().status()).isEqualTo(RestStatus.OK);

        GetResponse getResponse = fetchDocument(indexName, fnr);
        assertThat(getResponse.isExists()).isTrue();
        assertThat((boolean)getResponse.getSourceAsMap().get("foo")).isTrue();
    }

}