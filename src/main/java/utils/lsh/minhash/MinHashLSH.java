package utils.lsh.minhash;

import core.BaseSequence;
import utils.lsh.LSH;
import utils.lsh.PseudoPermutation;
import utils.lsh.storage.LSHStorage;
import utils.lsh.storage.minhash.AmplifiedMinHashStorage;
import utils.lsh.storage.minhash.BloomFilterHashStorage;
import utils.lsh.storage.minhash.LightHashStorage;
import utils.lsh.storage.minhash.TraditionalHashStorage;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class MinHashLSH<T, S extends LSHStorage<Long>> implements LSH<T> {
    protected final AmplifiedMinHashStorage<S> storage;

    protected final static long PRIME = 16777619L;
    protected final static long START_HASH = 2166136261L;
    protected final int k;
    protected final int b;
    protected final int bandSize;
    protected final PseudoPermutation[] permutations;
    protected final Function<T, long[]> kmerFunc;


    public MinHashLSH(int k, int r, int b, Function<T, long[]> kmerFunc, AmplifiedMinHashStorage<S> minHashStorage) {
        if (r % b != 0)
            throw new RuntimeException("r must be a multiple of b");
        if (k > 33)
            throw new RuntimeException("this LSH only supports k-mers up to k = 33");

        this.k = k;
        this.b = b;
        this.bandSize = r / b;

        this.storage = minHashStorage;

        long kMers = (long) Math.pow(4, k);
        this.kmerFunc = kmerFunc;
        this.permutations = Stream.iterate(new PseudoPermutation(kMers, kMers), p -> new PseudoPermutation(kMers, p.getP())).limit(r).toArray(PseudoPermutation[]::new);
    }

    public MinHashLSH(int k, int r, int b, Function<T, long[]> kmerFunc, S minHashStorage) {
        if (r % b != 0)
            throw new RuntimeException("r must be a multiple of b");
        if (k > 33)
            throw new RuntimeException("this LSH only supports k-mers up to k = 33");

        this.k = k;
        this.b = b;
        this.bandSize = r / b;

        this.storage = new AmplifiedMinHashStorage<>(1, LSHStorage.AmplifiedLSHStorage.Amplification.AND, () -> minHashStorage);

        long kMers = (long) Math.pow(4, k);
        this.kmerFunc = kmerFunc;
        this.permutations = Stream.iterate(new PseudoPermutation(kMers, kMers), p -> new PseudoPermutation(kMers, p.getP())).limit(r).toArray(PseudoPermutation[]::new);
    }

    @Override
    public AmplifiedMinHashStorage<S> getStorage() {
        return storage;
    }

    public S getInnerStorage(int bandId) {
        return storage.bands().get(bandId);
    }

    public static Traditional<BaseSequence> newSeqAmpLSHTraditional(int k, int r, int b, LSHStorage.AmplifiedLSHStorage.Amplification amp) {
        return new Traditional<>(
                k,
                r,
                b,
                seq -> seq.kmers(k).stream().mapToLong(BaseSequence::toBase4).toArray(),
                amp
        );
    }

    public static Light<BaseSequence> newSeqAmpLSHLight(int k, int r, int b, LSHStorage.AmplifiedLSHStorage.Amplification amp) {
        return new Light<>(
                k,
                r,
                b,
                seq -> seq.kmers(k).stream().mapToLong(BaseSequence::toBase4).toArray(),
                amp
        );
    }

    public static Bloom<BaseSequence> newSeqAmpLSHBloom(int k, int r, int b, long numBits, long numHashFunctions, LSHStorage.AmplifiedLSHStorage.Amplification amp) {
        return new Bloom<>(
                k,
                r,
                b,
                numBits,
                numHashFunctions,
                seq -> seq.kmers(k).stream().mapToLong(BaseSequence::toBase4).toArray(),
                amp
        );
    }

    public static Traditional<BaseSequence> newSeqLSHTraditional(int k, int r) {
        return newSeqAmpLSHTraditional(
                k,
                r,
                1,
                LSHStorage.AmplifiedLSHStorage.Amplification.AND
        );
    }

    public static Light<BaseSequence> newSeqLSHLight(int k, int r) {
        return newSeqAmpLSHLight(
                k,
                r,
                1,
                LSHStorage.AmplifiedLSHStorage.Amplification.AND
        );
    }

    public static Bloom<BaseSequence> newSeqLSHBloom(int k, int r, long numBits, long numHashFunctions) {
        return newSeqAmpLSHBloom(
                k,
                r,
                1,
                numBits,
                numHashFunctions,
                LSHStorage.AmplifiedLSHStorage.Amplification.AND
        );
    }

    protected long[] kmers(T t) {
        return kmerFunc.apply(t);
    }

    /**
     * Inserts a given element into this LSH instance.
     * @param t the element to insert.
     */
    @Override
    public void insert(T t) {
        storage.store(hashedSignatures(t));
    }

    @Override
    public void remove(T t) {
        storage.remove(hashedSignatures(t));
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

    public long[] hashedSignatures(T t) {
        return Arrays.stream(signatures(t)).mapToLong(MinHashLSH::hashSignature).toArray();
    }

    public static long hashSignature(long[] arr) {
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


    public boolean query(T t, LSHStorage.AmplifiedLSHStorage.Amplification amp) {
        long[] sigs = hashedSignatures(t);
        return storage.query(sigs, amp);
    }

    @Override
    public boolean query(T t) {
        long[] sigs = hashedSignatures(t);
        return storage.query(sigs, storage.amplification());
    }

    public static class Traditional<O> extends MinHashLSH<O, TraditionalHashStorage<Long, O>> {
        public Traditional(int k, int r, int b, Function<O, long[]> hashFunc, LSHStorage.AmplifiedLSHStorage.Amplification amp) {
            super(
                    k,
                    r,
                    b,
                    hashFunc,
                    AmplifiedMinHashStorage.newAmplifiedTraditionalMinHashStorage(b, amp)
            );
        }

        @Override
        public void insert(O o) {
            var bands = storage.bands();
            var sigs = hashedSignatures(o);
            IntStream.range(0, b).forEach(i -> bands.get(i).store(sigs[i], o));

        }

        public boolean queryExact(O o) {
            return candidates(o).contains(o);
        }

        @Override
        public void remove(O o) {
            var bands = storage.bands();
            var sigs = hashedSignatures(o);
            IntStream.range(0, b).forEach(i -> bands.get(i).remove(sigs[i], o));
        }

        public Set<O> candidates(O o) {
            return IntStream.range(0, b).mapToObj(i -> candidates(o, i)).flatMap(Collection::stream).collect(Collectors.toSet());
        }

        public Set<O> candidates(O o, int bandId) {
            return storage.bands().get(bandId).candidates(hashSignature(signatureOf(o, bandId)));
        }
    }

    public static class Bloom<O> extends MinHashLSH<O, BloomFilterHashStorage<Long>> {
        private final long numBits;
        private final long numHashFunctions;

        public Bloom(int k, int r, int b, long numBits, long numHashFunctions, Function<O, long[]> hashFunc, LSHStorage.AmplifiedLSHStorage.Amplification amp) {
            super(
                    k,
                    r,
                    b,
                    hashFunc,
                    AmplifiedMinHashStorage.newAmplifiedBloomFilterMinHashStorage(b, numBits, numHashFunctions, amp)
            );
            this.numBits = numBits;
            this.numHashFunctions = numHashFunctions;
        }

        public long getNumBits() {
            return numBits;
        }

        public long getNumHashFunctions() {
            return numHashFunctions;
        }
    }

    public static class Light<O> extends MinHashLSH<O, LightHashStorage<Long>> {
        public Light(int k, int r, int b, Function<O, long[]> hashFunc, LSHStorage.AmplifiedLSHStorage.Amplification amp) {
            super(
                    k,
                    r,
                    b,
                    hashFunc,
                    AmplifiedMinHashStorage.newAmplifiedLightMinHashStorage(b, amp)
            );

        }
        public Set<Long> hashSet() {
            return this.storage.bands().stream().flatMap(s -> s.hashSet().stream()).collect(Collectors.toSet());
        }

        public Set<Long> hashSet(int bandId) {
            return Collections.unmodifiableSet(this.storage.band(bandId).hashSet());
        }
    }
}