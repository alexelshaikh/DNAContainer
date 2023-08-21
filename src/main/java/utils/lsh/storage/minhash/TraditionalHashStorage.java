package utils.lsh.storage.minhash;

import utils.lsh.storage.LSHStorage;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TraditionalHashStorage<H, O> implements LSHStorage<H> {

    protected final Map<H, Set<O>> mappings;

    public TraditionalHashStorage() {
        this.mappings = new ConcurrentHashMap<>();
    }

    @Override
    public void remove(H hash) {
        mappings.remove(hash);
    }
    public void remove(H hash, O object) {
        Set<O> candidates = mappings.get(hash);
        if (candidates == null)
            return;
        if (candidates.isEmpty())
            mappings.remove(hash);

        candidates.remove(object);
    }

    @Override
    public void store(H hash) {
        this.mappings.computeIfAbsent(hash, __ -> ConcurrentHashMap.newKeySet());
    }

    public void store(H hash, O object) {
        this.mappings.computeIfAbsent(hash, __ -> ConcurrentHashMap.newKeySet()).add(object);
    }

    public Map<H, Set<O>> getMappings() {
        return mappings;
    }

    @Override
    public boolean query(H hash) {
        return !candidates(hash).isEmpty();
    }

    public boolean query(H hash, O object) {
        return candidates(hash).contains(object);
    }

    public Set<O> candidates(H hash) {
        Set<O> candidates = this.mappings.get(hash);
        return candidates != null ? candidates : Collections.emptySet();
    }
}

