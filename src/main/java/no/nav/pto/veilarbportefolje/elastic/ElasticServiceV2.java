package no.nav.pto.veilarbportefolje.elastic;

import lombok.extern.slf4j.Slf4j;
import no.nav.pto.veilarbportefolje.domene.Fnr;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static no.nav.pto.veilarbportefolje.util.ExceptionUtils.sneakyThrow;
import static org.elasticsearch.client.RequestOptions.DEFAULT;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Slf4j
public class ElasticServiceV2 {

    private final String alias;
    private final RestHighLevelClient restHighLevelClient;

    public ElasticServiceV2(RestHighLevelClient restHighLevelClient, String alias) {
        this.restHighLevelClient = restHighLevelClient;
        this.alias = alias;
    }

    public void updateHarDeltCv(Fnr fnr, boolean harDeltCv) {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(alias);
        updateRequest.type("_doc");
        updateRequest.id(fnr.getFnr());

        try {
            updateRequest.doc(jsonBuilder()
                    .startObject()
                    .field("har_delt_cv", harDeltCv)
                    .endObject()
            );
            restHighLevelClient.update(updateRequest, DEFAULT);

        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                log.info("Kunne ikke finne dokument ved oppdatering av cv");
            }
        } catch (IOException e) {
            sneakyThrow(e);
        }
    }
}
