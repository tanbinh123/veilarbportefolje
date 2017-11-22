package no.nav.fo.internal;


import org.junit.Test;

import javax.servlet.http.HttpServletRequest;

import java.util.Base64;

import static no.nav.fo.internal.AuthorizationUtils.AUTHORIZATION;
import static no.nav.fo.internal.AuthorizationUtils.SYSTEMUSER_PASSWORD;
import static no.nav.fo.internal.AuthorizationUtils.SYSTEMUSER_USERNAME;
import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthorizationUtilsTest {

    @Test
    public void skalIkkeGodtaRequest() {
        System.setProperty(SYSTEMUSER_USERNAME, "dontcare");
        System.setProperty(SYSTEMUSER_PASSWORD, "dontcare");

        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getHeader(AUTHORIZATION)).thenReturn("");
        assertThat(AuthorizationUtils.isBasicAuthAuthorized(request)).isFalse();
    }

    @Test
    public void skalGodtaRequest() {
        Base64.Encoder encoder = Base64.getEncoder();

        System.setProperty(SYSTEMUSER_USERNAME, "brukernavn");
        System.setProperty(SYSTEMUSER_PASSWORD, "passord");
        
        String basicAuth = "brukernavn:passord";
        String basicAuthDecoded = new String(encoder.encode(basicAuth.getBytes()));
        String authHeader = "Basic " + basicAuthDecoded;

        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getHeader(AUTHORIZATION)).thenReturn(authHeader);

        assertThat(AuthorizationUtils.isBasicAuthAuthorized(request)).isTrue();
    }
}