package no.nav.pto.veilarbportefolje.util;

import no.nav.pto.veilarbportefolje.domene.Bruker;
import no.nav.pto.veilarbportefolje.domene.Portefolje;
import no.nav.pto.veilarbportefolje.abac.PepClient;

import java.util.List;

import static java.util.stream.Collectors.toList;

public class PortefoljeUtils {

    public static Portefolje buildPortefolje(int antall, List<Bruker> brukereSublist, String enhet, int fra) {

        return new Portefolje()
                .setEnhet(enhet)
                .setBrukere(brukereSublist)
                .setAntallTotalt(antall)
                .setAntallReturnert(brukereSublist.size())
                .setFraIndex(fra);
    }

    public static List<Bruker> sensurerBrukere(List<Bruker> brukere, String token, PepClient pepClient) {
        return brukere.stream()
                .map( bruker -> fjernKonfidensiellInfoDersomIkkeTilgang(bruker, token, pepClient))
                .collect(toList());
    }

    private static Bruker fjernKonfidensiellInfoDersomIkkeTilgang(Bruker bruker, String token, PepClient pepClient) {
        if(!bruker.erKonfidensiell()) {
            return bruker;
        }

        String diskresjonskode = bruker.getDiskresjonskode();

        if("6".equals(diskresjonskode) && !pepClient.isSubjectAuthorizedToSeeKode6(token)) {
            return fjernKonfidensiellInfo(bruker);
        }
        if("7".equals(diskresjonskode) && !pepClient.isSubjectAuthorizedToSeeKode7(token)) {
            return fjernKonfidensiellInfo(bruker);
        }
        if(bruker.isEgenAnsatt() && !pepClient.isSubjectAuthorizedToSeeEgenAnsatt(token)) {
            return fjernKonfidensiellInfo(bruker);
        }
        return bruker;

    }

    private static Bruker fjernKonfidensiellInfo(Bruker bruker) {
        return bruker.setFnr("").setEtternavn("").setFornavn("").setKjonn("").setFodselsdato(null);
    }
}