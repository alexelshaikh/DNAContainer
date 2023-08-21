package utils.lsh.storage.minhash;

import utils.lsh.storage.LSHStorage;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LightHashStorage<H> implements LSHStorage<H> {

    private final Set<H> hashes;

    public LightHashStorage() {
        this.hashes = ConcurrentHashMap.newKeySet();
    }

    @Override
    public void remove(H hash) {
        hashes.remove(hash);
    }

    @Override
    public void store(H hash) {
        hashes.add(hash);
    }

    @Override
    public boolean query(H hash) {
        return hashes.contains(hash);
    }

    public Set<H> hashSet() {
        return hashes;
    }
}
