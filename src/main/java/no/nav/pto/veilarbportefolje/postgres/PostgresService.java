package no.nav.pto.veilarbportefolje.postgres;

import no.nav.common.types.identer.EnhetId;
import no.nav.pto.veilarbportefolje.client.VeilarbVeilederClient;
import no.nav.pto.veilarbportefolje.database.PostgresTable;
import no.nav.pto.veilarbportefolje.domene.*;
import no.nav.pto.veilarbportefolje.util.VedtakstottePilotRequest;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

@Service
public class PostgresService {
    private final VedtakstottePilotRequest vedtakstottePilotRequest;
    private final VeilarbVeilederClient veilarbVeilederClient;
    private final JdbcTemplate jdbcTemplate;

    public PostgresService(VedtakstottePilotRequest vedtakstottePilotRequest, @Qualifier("PostgresJdbcReadOnly") JdbcTemplate jdbcTemplate, VeilarbVeilederClient veilarbVeilederClient) {
        this.vedtakstottePilotRequest = vedtakstottePilotRequest;
        this.veilarbVeilederClient = veilarbVeilederClient;
        this.jdbcTemplate = jdbcTemplate;
    }

    public BrukereMedAntall hentBrukere(String enhetId, String veilederIdent, String sortOrder, String sortField, Filtervalg filtervalg, Integer fra, Integer antall) {
        List<String> veiledereMedTilgangTilEnhet = veilarbVeilederClient.hentVeilederePaaEnhet(EnhetId.of(enhetId));
        boolean vedtaksPilot = erVedtakstottePilotPa(EnhetId.of(enhetId));

        PostgresQueryBuilder query = new PostgresQueryBuilder(jdbcTemplate, enhetId, vedtaksPilot);

        boolean kallesFraMinOversikt = StringUtils.isNotBlank(veilederIdent);
        if (kallesFraMinOversikt) {
            query.minOversiktFilter(veilederIdent);
        }

        if (filtervalg.harAktiveFilter()) {
            if (filtervalg.harFerdigFilter()) {
                filtervalg.ferdigfilterListe.forEach(
                        filter -> leggTilFerdigFilter(query, filter, veiledereMedTilgangTilEnhet, vedtaksPilot)
                );
            }
            leggTilManuelleFilter(query, filtervalg);
        }

        query.sorterQueryParametere(sortOrder, sortField, filtervalg, kallesFraMinOversikt);
        return query.search(fra, antall);
    }

    private void leggTilManuelleFilter(PostgresQueryBuilder query, Filtervalg filtervalg) {
        List<Integer> fodseldagIMndQuery = filtervalg.fodselsdagIMnd.stream().map(Integer::parseInt).collect(toList());
        query.leggTilFodselsdagFilter(fodseldagIMndQuery);

        query.leggTilListeFilter(filtervalg.arbeidslisteKategori, PostgresTable.BRUKER_VIEW.ARB_KATEGORI);
        query.leggTilListeFilter(filtervalg.veiledere, PostgresTable.BRUKER_VIEW.VEILEDERID);
        query.leggTilListeFilter(filtervalg.formidlingsgruppe, PostgresTable.BRUKER_VIEW.FORMIDLINGSGRUPPEKODE);
        query.leggTilListeFilter(filtervalg.innsatsgruppe, PostgresTable.BRUKER_VIEW.KVALIFISERINGSGRUPPEKODE); // TODO: er det litt rart at disse to går mot samme felt?
        query.leggTilListeFilter(filtervalg.servicegruppe, PostgresTable.BRUKER_VIEW.KVALIFISERINGSGRUPPEKODE); // TODO: er det litt rart at disse to går mot samme felt?
        query.leggTilListeFilter(filtervalg.hovedmal, PostgresTable.BRUKER_VIEW.HOVEDMAALKODE);
        query.leggTilListeFilter(filtervalg.rettighetsgruppe, PostgresTable.BRUKER_VIEW.RETTIGHETSGRUPPEKODE);
        query.leggTilListeFilter(filtervalg.registreringstype, PostgresTable.BRUKER_VIEW.BRUKERS_SITUASJON);
        query.leggTilListeFilter(filtervalg.utdanning, PostgresTable.BRUKER_VIEW.UTDANNING);
        query.leggTilListeFilter(filtervalg.utdanningBestatt, PostgresTable.BRUKER_VIEW.UTDANNING_BESTATT);
        query.leggTilListeFilter(filtervalg.utdanningGodkjent, PostgresTable.BRUKER_VIEW.UTDANNING_GODKJENT);

        if (filtervalg.harNavnEllerFnrQuery()) {
            query.navnOgFodselsnummerSok(filtervalg.getNavnEllerFnrQuery());
        }
        if (!filtervalg.alder.isEmpty()) {
            query.alderFilter(filtervalg.alder);
        }
        if (filtervalg.harKjonnfilter()) {
            query.kjonnfilter(filtervalg.kjonn);
        }
        if (filtervalg.harManuellBrukerStatus()) {
            query.erManuell();
        }

        if (filtervalg.harCvFilter()) {
            if (filtervalg.cvJobbprofil.equals(CVjobbprofil.HAR_DELT_CV)) {
                query.harDeltCvFilter();
            } else if (filtervalg.cvJobbprofil.equals(CVjobbprofil.HAR_IKKE_DELT_CV)) {
                query.harIkkeDeltCvFilter();
            }
        }

        if (!filtervalg.veiledere.isEmpty()) {
            query.veiledereFilter(filtervalg.veiledere);
        }

        if (!filtervalg.tiltakstyper.isEmpty()) {
            query.tiltaksTyperFilter(filtervalg.tiltakstyper);
        }

        if (!filtervalg.aktiviteterForenklet.isEmpty()) {
            query.aktiviteterForenkletFilter(filtervalg.aktiviteterForenklet);
        }

        if (filtervalg.harYtelsefilter()) {
            query.ytelserFilter(filtervalg.ytelse.underytelser);
        }

        if (filtervalg.harAktivitetFilter()) {
            query.aktivitetFilter(filtervalg.aktiviteter);
        }

        if (filtervalg.harUlesteEndringerFilter()) {
            query.ulesteEndringerFilter();
        }

        if (filtervalg.harSisteEndringFilter()) {
            query.sisteEndringFilter(filtervalg.sisteEndringKategori);
        }
    }


    static QueryBuilder leggTilFerdigFilter(PostgresQueryBuilder query, Brukerstatus brukerStatus, List<String> veiledereMedTilgangTilEnhet, boolean erVedtakstottePilotPa) {
        switch (brukerStatus) {
            case UFORDELTE_BRUKERE:
                query.ufordeltBruker(veiledereMedTilgangTilEnhet);
                break;
            case NYE_BRUKERE_FOR_VEILEDER:
                query.nyForVeileder();
                break;
            case INAKTIVE_BRUKERE:
                query.ikkeServiceBehov();
                break;
            case VENTER_PA_SVAR_FRA_NAV:
                query.venterPaSvarFraNav();
                break;
            case VENTER_PA_SVAR_FRA_BRUKER:
                query.venterPaSvarFraBruker();
                break;
            case I_AVTALT_AKTIVITET:
                query.iavtaltAktivitet();
                break;
            case IKKE_I_AVTALT_AKTIVITET:
                query.ikkeIAvtaltAktivitet();
                break;
            case UTLOPTE_AKTIVITETER:
                query.utlopteAktivitet();
                break;
            case MIN_ARBEIDSLISTE:
                query.harArbeidsliste();
                break;
            case MOTER_IDAG:
                query.moterIDag();
                break;
            case ER_SYKMELDT_MED_ARBEIDSGIVER:
                query.erSykmeldtMedArbeidsgiver(erVedtakstottePilotPa);
                break;
            case TRENGER_VURDERING:
                query.trengerVurdering(erVedtakstottePilotPa);
                break;
            case UNDER_VURDERING:
                query.underVurdering(erVedtakstottePilotPa);
                break;
            default:
                throw new IllegalStateException();

        }
        return null;
    }

    public List<Bruker> hentBrukereMedArbeidsliste(String veilederId, String enhetId) {
        return new ArrayList<>();
    }

    public StatusTall hentStatusTallForVeileder(String veilederId, String enhetId) {
        boolean vedtakstottePilotErPa = erVedtakstottePilotPa(EnhetId.of(enhetId));
        return new StatusTall();
    }

    public StatusTall hentStatusTallForEnhet(String enhetId) {
        return new StatusTall();
    }

    public FacetResults hentPortefoljestorrelser(String enhetId) {
        return null;
    }

    private boolean erVedtakstottePilotPa(EnhetId enhetId) {
        return vedtakstottePilotRequest.erVedtakstottePilotPa(enhetId);
    }
}
