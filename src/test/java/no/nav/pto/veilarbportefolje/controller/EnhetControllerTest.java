package no.nav.pto.veilarbportefolje.controller;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import com.nimbusds.jwt.SignedJWT;
import lombok.SneakyThrows;
import no.nav.common.abac.Pep;
import no.nav.common.auth.context.AuthContext;
import no.nav.common.auth.context.AuthContextHolder;
import no.nav.common.auth.context.AuthContextHolderThreadLocal;
import no.nav.common.auth.context.UserRole;
import no.nav.common.metrics.MetricsClient;
import no.nav.common.types.identer.EnhetId;
import no.nav.common.types.identer.NavIdent;
import no.nav.pto.veilarbportefolje.arenapakafka.aktiviteter.TiltakServiceV2;
import no.nav.pto.veilarbportefolje.auth.AuthService;
import no.nav.pto.veilarbportefolje.auth.ModiaPep;
import no.nav.pto.veilarbportefolje.domene.BrukereMedAntall;
import no.nav.pto.veilarbportefolje.domene.Filtervalg;
import no.nav.pto.veilarbportefolje.opensearch.OpensearchService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.web.server.ResponseStatusException;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.util.Collections;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EnhetControllerTest {

    private OpensearchService opensearchService;
    private EnhetController enhetController;
    private Pep pep;
    private ModiaPep modiaPep;
    private AuthContextHolder authContextHolder;

    @Before
    public void initController() {
        opensearchService = mock(OpensearchService.class);
        pep = mock(Pep.class);
        modiaPep = mock(ModiaPep.class);
        authContextHolder = AuthContextHolderThreadLocal.instance();

        AuthService authService = new AuthService(pep, modiaPep);
        enhetController = new EnhetController(opensearchService, authService, mock(TiltakServiceV2.class), mock(MetricsClient.class));
    }

    @Test
    @SneakyThrows
    public void skal_hent_portefolje_fra_indeks_dersom_tilgang() {
        when(modiaPep.harVeilederTilgangTilModia(anyString())).thenReturn(true);
        when(pep.harVeilederTilgangTilEnhet(any(NavIdent.class), any(EnhetId.class))).thenReturn(true);
        when(opensearchService.hentBrukere(any(), any(), any(), any(), any(), any(), any())).thenReturn(new BrukereMedAntall(0, Collections.emptyList()));

        authContextHolder.withContext(
                new AuthContext(UserRole.INTERN, generateMockJWT()),
                () -> enhetController.hentPortefoljeForEnhet("0001", 0, 0, "ikke_satt", "ikke_satt", new Filtervalg())
        );
        verify(opensearchService, times(1)).hentBrukere(any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    public void skal_hente_hele_portefolje_fra_indeks_dersom_man_mangle_antall() {
        when(pep.harVeilederTilgangTilEnhet(any(), any())).thenReturn(true);
        when(modiaPep.harVeilederTilgangTilModia(any())).thenReturn(true);
        when(opensearchService.hentBrukere(any(), any(), any(), any(), any(), any(), any())).thenReturn(new BrukereMedAntall(0, Collections.emptyList()));

        authContextHolder.withContext(
                new AuthContext(UserRole.INTERN, generateMockJWT()),
                () -> enhetController.hentPortefoljeForEnhet("0001", 0, null, "ikke_satt", "ikke_satt", new Filtervalg())
        );
        verify(opensearchService, times(1)).hentBrukere(any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    public void skal_hente_hele_portefolje_fra_indeks_dersom_man_mangle_fra() {
        when(pep.harVeilederTilgangTilEnhet(any(), any())).thenReturn(true);
        when(modiaPep.harVeilederTilgangTilModia(any())).thenReturn(true);
        when(opensearchService.hentBrukere(any(), any(), any(), any(), any(), any(), any())).thenReturn(new BrukereMedAntall(0, Collections.emptyList()));
        authContextHolder
                .withContext(
                        new AuthContext(UserRole.INTERN, generateMockJWT()),
                        () -> enhetController.hentPortefoljeForEnhet("0001", null, 20, "ikke_satt", "ikke_satt", new Filtervalg())
                );

        verify(opensearchService, times(1)).hentBrukere(any(), any(), any(), any(), any(), isNull(), any());
    }

    @Test(expected = ResponseStatusException.class)
    public void skal_ikke_hente_noe_hvis_mangler_tilgang() {
        when(modiaPep.harVeilederTilgangTilModia(any())).thenReturn(false);
        authContextHolder.withContext(
                new AuthContext(UserRole.INTERN, generateMockJWT()),
                () -> enhetController.hentPortefoljeForEnhet("0001", null, 20, "ikke_satt", "ikke_satt", new Filtervalg())
        );
    }

    @SneakyThrows
    private JWT generateMockJWT() {
        JWTClaimsSet claimsSet = new JWTClaimsSet.Builder().claim("NAVident", "A111111").build();
        JWSHeader header = new JWSHeader.Builder(new JWSAlgorithm("RS256"))
                .type(JOSEObjectType.JWT)
                .build();
        SignedJWT jwt = new SignedJWT(header, claimsSet);
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048, new SecureRandom("mock_key".getBytes()));
        KeyPair keyPair = generator.generateKeyPair();
        jwt.sign(new RSASSASigner(keyPair.getPrivate()));

        return JWTParser.parse(jwt.serialize());
    }
}
