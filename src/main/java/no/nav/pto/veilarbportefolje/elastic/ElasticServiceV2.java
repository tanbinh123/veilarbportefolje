package no.nav.pto.veilarbportefolje.elastic;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import no.nav.pto.veilarbportefolje.domene.Fnr;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.elasticsearch.client.RequestOptions.DEFAULT;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Slf4j
public class ElasticServiceV2 {

    private final String index;
    private final RestHighLevelClient restHighLevelClient;

    public ElasticServiceV2(RestHighLevelClient restHighLevelClient, String index) {
        this.restHighLevelClient = restHighLevelClient;
        this.index = index;
    }

    @SneakyThrows
    public void updateHarDeltCv(Fnr fnr, boolean harDeltCv) {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("har_delt_cv", harDeltCv)
                .endObject();

        send(updateRequest(fnr, index, builder));
    }

    @SneakyThrows
    public void avsluttOppfolging(Fnr fnr) {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("oppfolging", false)
                .endObject();

        send(updateRequest(fnr, index, builder));
    }

    private void send(UpdateRequest updateRequest) throws IOException {
        try {
            restHighLevelClient.update(updateRequest, DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                log.warn("Kunne ikke finne dokument ved oppdatering av cv");
            }
        }
    }

    private static UpdateRequest updateRequest(Fnr fnr, String alias, XContentBuilder builder) {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(alias);
        updateRequest.type("_doc");
        updateRequest.id(fnr.getFnr());
        updateRequest.doc(builder);
        return updateRequest;
    }
}
