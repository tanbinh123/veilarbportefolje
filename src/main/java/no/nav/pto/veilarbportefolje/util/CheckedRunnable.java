package no.nav.pto.veilarbportefolje.util;

@FunctionalInterface
public interface CheckedRunnable<E extends Exception> {
    void run() throws E;
}
