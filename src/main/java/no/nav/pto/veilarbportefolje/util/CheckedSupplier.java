package no.nav.pto.veilarbportefolje.util;

@FunctionalInterface
public interface CheckedSupplier<R, E extends Exception> {
    R get() throws E;
}
