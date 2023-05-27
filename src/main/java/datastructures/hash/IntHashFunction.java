package datastructures.hash;

import java.util.Objects;
import java.util.function.Function;

public interface IntHashFunction<T> extends Function<T, Integer> {

    static <F> IntHashFunction<F> hashCodeMapping() {
        return Objects::hashCode;
    }

    default int applyMod(T t, int mod) {
        return apply(t) % mod;
    }
}

interface LongHashFunction<T> extends Function<T, Long> {

    static <F> IntHashFunction<F> hashCodeMapping() {
        return Objects::hashCode;
    }

    default long applyMod(T t, long mod) {
        return apply(t) % mod;
    }
}
