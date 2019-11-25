package no.nav.fo.veilarbportefolje.provider.rest;

import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import no.nav.brukerdialog.security.domain.IdentType;
import no.nav.common.auth.SubjectHandler;
import no.nav.fo.veilarbportefolje.database.BrukerRepository;
import no.nav.fo.veilarbportefolje.domene.Fnr;
import no.nav.fo.veilarbportefolje.domene.OppfolgingEnhetDTO;
import no.nav.fo.veilarbportefolje.domene.OppfolgingEnhetPageDTO;
import no.nav.fo.veilarbportefolje.service.AktoerService;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.ws.rs.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static no.nav.brukerdialog.security.domain.IdentType.InternBruker;
import static no.nav.brukerdialog.security.domain.IdentType.Systemressurs;

@Slf4j
@Api(value = "OppfolgingEnhet")
@Path("/oppfolgingenhet")
@Component
@Produces(APPLICATION_JSON)
public class OppfolgingenhetRessurs {

    static final int PAGE_SIZE_MAX = 1000;
    private static final int PAGE_NUMBER_MAX = 500_000;

    private BrukerRepository brukerRepository;
    private AktoerService aktoerService;

    @Inject
    public OppfolgingenhetRessurs(BrukerRepository brukerRepository, AktoerService aktoerService) {
        this.brukerRepository = brukerRepository;
        this.aktoerService = aktoerService;
    }

    @GET
    public OppfolgingEnhetPageDTO getOppfolgingEnhet(@DefaultValue("1") @QueryParam("page_number") int pageNumber, @DefaultValue("10") @QueryParam("page_size") int pageSize) {

        autoriserBruker();

        Integer totalNumberOfUsers = brukerRepository.hentAntallBrukereUnderOppfolging().orElseThrow(() -> new WebApplicationException(503));
        long totalNumberOfPages = new BigDecimal(totalNumberOfUsers).divide(new BigDecimal(pageSize), RoundingMode.UP).longValue() + 1;

        validatePageSize(pageSize);
        validatePageNumber(pageNumber, totalNumberOfPages);

        List<OppfolgingEnhetDTO> brukereMedOppfolgingsEnhet = brukerRepository.hentBrukereUnderOppfolging(pageNumber, pageSize);

        brukereMedOppfolgingsEnhet.forEach(bruker -> {
            if (bruker.getAktorId() == null) {
                String aktorId = hentAktoerIdFraAktoerService(bruker);
                bruker.setAktorId(aktorId);
            }
        });

        return OppfolgingEnhetPageDTO.builder()
                .page_number(pageNumber)
                .page_number_total(totalNumberOfPages)
                .number_of_users(brukereMedOppfolgingsEnhet.size())
                .users(brukereMedOppfolgingsEnhet)
                .build();
    }

    private String hentAktoerIdFraAktoerService(OppfolgingEnhetDTO bruker) {
        return aktoerService.hentAktoeridFraFnr(Fnr.of(bruker.getFnr()))
                .getOrElseThrow((Supplier<RuntimeException>) RuntimeException::new)
                .toString();
    }

    private void autoriserBruker() {
        IdentType identType = SubjectHandler.getIdentType().orElseThrow(NotFoundException::new);
        String ident = SubjectHandler.getIdent().orElseThrow(NotFoundException::new);

        if (ugyldigIdent(identType, ident)) {
            throw new NotFoundException();
        }
    }

    static boolean ugyldigIdent(IdentType identType, String ident) {
        List<IdentType> internBrukere = Arrays.asList(InternBruker, Systemressurs);
        if (!internBrukere.contains(identType) || !"srvveilarboppfolging".equals(ident)) {
            log.warn("Ident med navn {} og type {} er ugyldig", ident, identType);
            return true;
        } else {
            return false;
        }
    }

    static void validatePageNumber(int pageNumber, long pagesTotal) {

        if (pageNumber < 1) {
            throw new WebApplicationException("Page number is below 1", 400);
        }

        if (pageNumber > pagesTotal) {
            throw new WebApplicationException("Page number is higher than total number of pages", 404);
        }

        if (pageNumber > PAGE_NUMBER_MAX) {
            throw new WebApplicationException("Page number exceeds max limit", 400);
        }
    }

    static void validatePageSize(int pageSize) {

        if (pageSize < 1) {
            throw new WebApplicationException("Page size too small", 400);
        }

        if (pageSize > PAGE_SIZE_MAX) {
            throw new WebApplicationException("Page size exceeds max limit", 400);
        }
    }

}