package no.nav.pto.veilarbportefolje.util;

@FunctionalInterface
public interface CheckedFunction<T, R, E extends Exception> {
    R apply(T var1) throws E;
}

