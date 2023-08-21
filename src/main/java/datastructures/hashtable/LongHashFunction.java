package datastructures.hashtable;

import java.util.function.Function;

public interface LongHashFunction<T> extends Function<T, Long> {

    default long applyMod(T t, long mod) {
        return apply(t) % mod;
    }
}
