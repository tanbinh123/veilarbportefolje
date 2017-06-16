package no.nav.fo.provider.rest.arbeidsliste;

import io.swagger.annotations.Api;
import no.nav.fo.domene.Arbeidsliste;
import no.nav.fo.domene.Fnr;
import no.nav.fo.provider.rest.arbeidsliste.exception.ArbeidslisteIkkeFunnetException;
import no.nav.fo.provider.rest.arbeidsliste.exception.ArbeidslisteIkkeOppdatertException;
import no.nav.fo.provider.rest.arbeidsliste.exception.ArbeidslisteIkkeOpprettetException;
import no.nav.fo.service.ArbeidslisteService;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.sql.Timestamp;
import java.time.Instant;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.CREATED;

@Api(value = "arbeidsliste")
@Path("/arbeidsliste/{fnr}")
@Produces(APPLICATION_JSON)
@Consumes(APPLICATION_JSON)
public class ArbeidsListeRessurs {

    @Inject
    private ArbeidslisteService arbeidslisteService;

    @GET
    public Response getArbeidsListe(@PathParam("fnr") String fnr) {
        Arbeidsliste arbeidsliste = arbeidslisteService
                .getArbeidsliste(new Fnr(fnr))
                .orElseThrow(ArbeidslisteIkkeFunnetException::new);

        return Response.ok().entity(arbeidsliste).build();
    }

    @PUT
    public Response putArbeidsListe(ArbeidslisteRequest body, @PathParam("fnr") String fnr) {
        ArbeidslisteUpdate updateData = createUpdateData(body, fnr);
        arbeidslisteService
                .createArbeidsliste(updateData)
                .orElseThrow(ArbeidslisteIkkeOpprettetException::new);

        return Response.status(CREATED).build();
    }

    @POST
    public Response postArbeidsListe(ArbeidslisteRequest body, @PathParam("fnr") String fnr) {
        ArbeidslisteUpdate updateData = createUpdateData(body, fnr);
        arbeidslisteService
                .updateArbeidsliste(updateData)
                .orElseThrow(ArbeidslisteIkkeOppdatertException::new);

        return Response.ok().build();
    }

    @DELETE
    public Response deleteArbeidsliste(@PathParam("fnr") String fnr) {
        arbeidslisteService
                .deleteArbeidsliste(new Fnr(fnr))
                .orElseThrow(ArbeidslisteIkkeFunnetException::new);

        return Response.ok().build();
    }

    private ArbeidslisteUpdate createUpdateData(ArbeidslisteRequest body, @PathParam("fnr") String fnr) {
        return new ArbeidslisteUpdate(new Fnr(fnr))
                .setVeilederId(body.getVeilederId())
                .setKommentar(body.getKommentar())
                .setFrist(Timestamp.from(Instant.parse(body.getFrist())));
    }
}
