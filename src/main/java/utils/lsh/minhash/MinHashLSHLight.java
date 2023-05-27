package utils.lsh.minhash;

import core.BaseSequence;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class MinHashLSHLight<T> extends AbstractMinHashLSH<T> {

    private final List<Set<Long>> bands;

    /**
     * Creates a tread-safe LSH light instance that supports concurrent insertion and querying. This instance reduces the memory overhead at the expense of more false negatives and false positives.
     * @param k the kmer length
     * @param r the number of hash functions (permutations)
     * @param b the number of bands. Note that for r=120 and b=10, the resulting bandSize is 12 i.e. 12 hash functions per band/signature
     */
    public MinHashLSHLight(int k, int r, int b, Function<T, long[]> kmerFunc) {
        super(k, r, b, kmerFunc);
        this.bands = Stream.generate((Supplier<Set<Long>>) ConcurrentHashMap::newKeySet).limit(b).toList();
    }

    public static MinHashLSHLight<BaseSequence> newLSHForBaseSequences(int k, int r, int b) {
        return new MinHashLSHLight<>(k, r, b, seq -> seq.kmers(k).stream().mapToLong(BaseSequence::toBase4).toArray());
    }

    @Override
    protected void insertIntoBand(T element, long[] signature, int bandId) {
        bands.get(bandId).add(hashSignature(signature));
    }

    @Override
    protected void removeFromBand(T element, long[] signature, int bandId) {
        bands.get(bandId).remove(hashSignature(signature));
    }

    @Override
    public boolean isPresent(T t) {
        var sigs = signatures(t);
        for (int band = 0; band < b; band++) {
            if (bands.get(band).contains(hashSignature(sigs[band])))
                return true;
        }
        return false;
    }
}
