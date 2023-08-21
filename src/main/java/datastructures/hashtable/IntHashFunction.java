package datastructures.hashtable;

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


