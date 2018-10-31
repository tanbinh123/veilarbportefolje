package no.nav.fo.mock;

import io.vavr.control.Try;
import no.nav.fo.domene.*;
import no.nav.fo.service.SolrService;

import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;

public class SolrServiceMock implements SolrService {

    @Override
    public void hovedindeksering() {
    }

    @Override
    public void deltaindeksering() {

    }

    @Override
    public BrukereMedAntall hentBrukere(String enhetId, Optional<String> veilederIdent, String sortOrder, String sortField, Filtervalg filtervalg, Integer fra, Integer antall) {
        return new BrukereMedAntall(0, emptyList());
    }

    @Override
    public BrukereMedAntall hentBrukere(String enhetId, Optional<String> veilederIdent, String sortOrder, String sortField, Filtervalg filtervalg) {
        return new BrukereMedAntall(0, emptyList());
    }

    @Override
    public void slettBruker(String fnr) {

    }

    @Override
    public void slettBruker(PersonId personid) {
        
    }
    
    @Override
    public void indekserBrukerdata(PersonId personId) {

    }

    @Override
    public StatusTall hentStatusTallForPortefolje(String enhet) {
        return null;
    }

    @Override
    public FacetResults hentPortefoljestorrelser(String enhetId) {
        return null;
    }

    @Override
    public StatusTall hentStatusTallForVeileder(String enhet, String veilederIdent) {
        return null;
    }

    @Override
    public Try<List<Bruker>> hentBrukereMedArbeidsliste(VeilederId veilederId, String enhet) {
        return null;
    }

    @Override
    public void indekserAsynkront(AktoerId aktoerId) {
    }

    @Override
    public void indekserBrukere(List<PersonId> personIds) {
    }

    @Override
    public void commit() {
    }
    
}