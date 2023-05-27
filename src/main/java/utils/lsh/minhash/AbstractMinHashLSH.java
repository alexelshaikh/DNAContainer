package utils.lsh.minhash;

import utils.lsh.LSH;
import utils.lsh.PseudoPermutation;
import java.util.function.Function;
import java.util.stream.Stream;

public abstract class AbstractMinHashLSH<T> implements LSH<T> {
    protected final static long PRIME = 16777619L;
    protected final static long START_HASH = 2166136261L;
    protected final int k;
    protected final int b;
    protected final int bandSize;
    protected final PseudoPermutation[] permutations;
    protected final Function<T, long[]> kmerFunc;


    /**
     * Creates a tread-safe LSH instance that supports concurrent insertion and querying
     * @param k the kmer length
     * @param r the number of hash functions (permutations)
     * @param b the number of bands. Note that for r=120 and b=10, the resulting bandSize is 12 i.e. 12 hash functions per band/signature
     */
    public AbstractMinHashLSH(int k, int r, int b, Function<T, long[]> kmerFunc) {
        if (r % b != 0)
            throw new RuntimeException("r must be a multiple of b");
        if (k > 33)
            throw new RuntimeException("this LSH only supports k-mers up to k = 33");

        this.k = k;
        this.b = b;
        this.bandSize = r / b;

        long kMers = (long) Math.pow(4, k);
        this.kmerFunc = kmerFunc;
        this.permutations = Stream.iterate(new PseudoPermutation(kMers, kMers), p -> new PseudoPermutation(kMers, p.getP())).limit(r).toArray(PseudoPermutation[]::new);
    }

    protected abstract void insertIntoBand(T element, long[] signature, int bandId);
    protected abstract void removeFromBand(T element, long[] signature, int bandId);


    protected long[] kmers(T t) {
        return kmerFunc.apply(t);
    }

    /**
     * Inserts a given element into this LSH instance.
     * @param t the element to insert.
     */
    @Override
    public void insert(T t) {
        long[][] sigs = signatures(t);
        for (int band = 0; band < b; band++)
            insertIntoBand(t, sigs[band], band);
    }

    @Override
    public void remove(T t) {
        long[][] sigs = signatures(t);
        for (int band = 0; band < b; band++)
            removeFromBand(t, sigs[band], band);
    }

    /**
     * @return the minHash values for the given element.
     */
    public long[] minHashesPerHashFunction(T t) {
        long[] kmers = kmers(t);
        long[] minHashes = new long[permutations.length];
        for (int i = 0; i < permutations.length; i++)
            minHashes[i] = calcMinHashOfFunction(kmers, permutations[i]);

        return minHashes;
    }

    protected long calcMinHashOfFunction(long[] kmers, PseudoPermutation p) {
        long minHash = Long.MAX_VALUE;
        long permHash;
        for (long kmer : kmers) {
            permHash = p.apply(kmer);
            if (permHash == 0L) {
                minHash = 0L;
                break;
            }
            if (permHash < minHash)
                minHash = permHash;
        }
        return minHash;
    }

    /**
     * @return the minHash values of the specified band, i.e., the signature of the band for the given element.
     */
    public long[] signatureOf(T t, int bandId) {
        long[] kmers = kmers(t);
        long[] minHashes = new long[bandSize];
        int start = bandId * bandSize;
        int last = start + bandSize;
        int c = 0;
        for (int i = start; i < last; i++)
            minHashes[c++] = calcMinHashOfFunction(kmers, permutations[i]);

        return minHashes;
    }

    /**
     * @param t the input element.
     * @return the signatures of each band for the input element.
     */
    public long[][] signatures(T t) {
        var kmers = kmers(t);
        long[][] sigs = new long[b][];
        int band = 0;
        int permId = 0;
        do {
            long[] sig = new long[bandSize];
            for (int i = 0; i < bandSize; i++)
                sig[i] = calcMinHashOfFunction(kmers, permutations[permId++]);

            sigs[band++] = sig;
        } while (permId < permutations.length);

        return sigs;
    }

    protected static long hashSignature(long[] arr) {
        long hash = START_HASH;
        for (long l : arr)
            hash = (hash ^ l) * PRIME;

        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        return hash;
    }

    public int getK() {
        return k;
    }

    public int getB() {
        return b;
    }

    public int getR() {
        return permutations.length;
    }
}
