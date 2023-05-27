package utils.lsh.minhash;

import core.BaseSequence;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class MinHashLSH<T> extends AbstractMinHashLSH<T> {

    protected final List<Map<Long, Set<T>>> bands;


    /**
     * Creates a tread-safe LSH instance that supports concurrent insertion and querying
     * @param k the kmer length
     * @param r the number of hash functions (permutations)
     * @param b the number of bands. Note that for r=120 and b=10, the resulting bandSize is 12 i.e. 12 hash functions per band/signature
     */
    public MinHashLSH(int k, int r, int b, Function<T, long[]> kmerFunc) {
        super(k, r, b, kmerFunc);
        this.bands = Stream.generate((Supplier<Map<Long, Set<T>>>) ConcurrentHashMap::new).limit(b).toList();
    }

    public static MinHashLSH<BaseSequence> newLSHForBaseSequences(int k, int r, int b) {
        return new MinHashLSH<>(k, r, b, seq -> seq.kmers(k).stream().mapToLong(BaseSequence::toBase4).toArray());
    }

    @Override
    protected void insertIntoBand(T t, long[] signature, int bandId) {
        var sigHash = hashSignature(signature);
        var band = bands.get(bandId);
        band.computeIfAbsent(sigHash, k1 -> ConcurrentHashMap.newKeySet()).add(t);
    }

    @Override
    protected void removeFromBand(T element, long[] signature, int bandId) {
        var sigHash = hashSignature(signature);
        var band = bands.get(bandId);
        Set<T> set = band.computeIfPresent(sigHash, (k, v) -> {
            v.remove(element);
            return v;
        });
        if (set != null && set.isEmpty()) // optimistic
            band.remove(sigHash);
    }

    /**
     * @param t the input element.
     * @return the set of similar elements this LSH instance matches for the input element.
     */
    public Set<T> similarElements(T t) {
        return similarElements(t, Integer.MAX_VALUE);
    }


    /**
     * @param t the input element.
     * @param maxCount the maximum number of matches. If maxCount matches are found, this method returns and does not search for more matches.
     * @return the set of similar elements this LSH instance matches for the input element. It will return maxCount matches at most.
     */
    public Set<T> similarElements(T t, int maxCount) {
        long[][] sigs = signatures(t);
        Set<T> result = new HashSet<>();
        Set<T> matches;
        for (int band = 0; band < b; band++) {
            var sigHash = hashSignature(sigs[band]);
            var currentBand = bands.get(band);
            matches = currentBand.get(sigHash);
            if (matches != null) {
                result.addAll(matches);
                if (result.size() >= maxCount)
                    return result;
            }
        }
        return result;
    }

    @Override
    public boolean isPresent(T t) {
        return !similarElements(t, 1).isEmpty();
    }
}
