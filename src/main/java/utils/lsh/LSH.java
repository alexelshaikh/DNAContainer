package utils.lsh;

import utils.FuncUtils;
import utils.lsh.storage.LSHStorage;

public interface LSH<T> {
    LSHStorage<?> getStorage();
    void insert(T item);
    void remove(T item);
    boolean query(T item);
    default void insertParallel(Iterable<T> it) {
        FuncUtils.stream(it).parallel().forEach(this::insert);
    }
    default void insert(Iterable<T> it) {
        it.forEach(this::insert);
    }
}
