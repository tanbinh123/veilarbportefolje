package no.nav.pto.veilarbportefolje.util;


import java.util.Optional;

public class ExceptionUtils {

    public static <T,R> Optional<R> sneakyThrows(T t, CheckedFunction<T, R, Exception> function) {
        try {
            return Optional.ofNullable(function.apply(t));
        } catch (Exception e) {
            ExceptionUtils.sneakyThrow(e);
        }
        throw new IllegalStateException();
    }

    public static void sneakyThrows(CheckedRunnable<Exception> runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            ExceptionUtils.sneakyThrow(e);
        }
    }

    public static <T> Optional<T> sneakyThrows(CheckedSupplier<T, Exception> supplier) {
        try {
            return Optional.ofNullable(supplier.get());
        } catch (Exception e) {
            ExceptionUtils.sneakyThrow(e);
        }
        throw new IllegalStateException();
    }


    public static RuntimeException sneakyThrow(Exception e) {
        return checkednessRemover(e);
    }

    private static <T extends Exception> T checkednessRemover(Exception e) throws T {
        throw (T) e;
    }
}


