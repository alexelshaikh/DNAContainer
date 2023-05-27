package utils.lsh;

import utils.FuncUtils;

public interface LSH<T> {
    void insert(T item);
    void remove(T item);
    boolean isPresent(T item);
    default void insertParallel(Iterable<T> it) {
        FuncUtils.stream(it).parallel().forEach(this::insert);
    }
    default void insert(Iterable<T> it) {
        it.forEach(this::insert);
    }
}
