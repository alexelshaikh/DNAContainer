package utils.lsh.storage.minhash;

import datastructures.hashtable.BloomFilter;
import utils.lsh.storage.LSHStorage;
import java.util.function.Function;

public class BloomFilterHashStorage<H> implements LSHStorage<H> {
    private final BloomFilter<H> bf;

    public BloomFilterHashStorage(long numBits, long numHashFunctions, Function<H, Long> hasher) {
        this.bf = new BloomFilter<>(numBits, numHashFunctions, hasher, true);
    }

    @Override
    public void remove(H hash) {
        throw new UnsupportedOperationException("cannot remove hash from Bloom Filter");
    }

    @Override
    public void store(H hash) {
        this.bf.insert(hash);
    }

    @Override
    public boolean query(H hash) {
        return this.bf.mightContain(hash);
    }
}
