package no.nav.pto.veilarbportefolje.elastic;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import no.nav.pto.veilarbportefolje.domene.Fnr;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.Optional;

import static no.nav.pto.veilarbportefolje.elastic.ElasticUtils.creatUpdateRequest;
import static org.elasticsearch.client.RequestOptions.DEFAULT;

@Slf4j
@Component
public class ElasticServiceV2 {

    private final RestHighLevelClient client;
    private final String alias;

    @Inject
    public ElasticServiceV2(RestHighLevelClient client, String alias) {
        this.client = client;
        this.alias = alias;
    }

    public Optional<UpdateResponse> upsert(Fnr fnr, boolean harDeltCv) {
        String json = new JSONObject()
                .put("har_delt_cv", harDeltCv)
                .toString();

        return upsert(fnr, json);
    }

    @SneakyThrows
    public Optional<UpdateResponse> upsert(Fnr fnr, String json) {
        UpdateRequest updateRequest = creatUpdateRequest(alias, fnr, json);
        UpdateResponse updateResponse = client.update(updateRequest, DEFAULT);
        return Optional.ofNullable(updateResponse);
    }
}
