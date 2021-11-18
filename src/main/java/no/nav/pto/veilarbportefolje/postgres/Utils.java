package no.nav.pto.veilarbportefolje.postgres;

import java.util.List;
import java.util.stream.Collectors;

public class Utils {
    public static String eq(String kolonne, boolean verdi) {
        if (verdi) {
            return kolonne;
        } else {
            return "NOT " + kolonne;
        }
    }

    public static String eq(String kolonne, String verdi) {
        return kolonne + " = '" + verdi + "'";
    }

    public static String eq(String kolonne, int verdi) {
        return kolonne + " = " + verdi;
    }

    public static String contains(String kolonne, List<String> verdier) {
        return kolonne + " IN " + verdier.stream().map(v -> "'" + v + "'").collect(Collectors.joining(",", "(", ")"));
    }
}
