package datastructures.hashtable;

import utils.BitSetXXL;
import utils.FuncUtils;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Stream;

public class BloomFilter<T> {
    private static final double LN_2 = Math.log(2);
    private static final double LN_2_SQUARED = LN_2 * LN_2;
    private static final boolean DEFAULT_PREALLOCATE = false;

    private final List<HF> hashFunctions;
    private final BitSetXXL bitSet;
    private final long numBits;
    private final Function<T, Long> hasher;

    public BloomFilter(long numBits, long numHashFunctions, boolean preallocate) {
        this(numBits, numHashFunctions, t -> (long) FuncUtils.consistentHash(t), preallocate);
    }

    public BloomFilter(long numBits, long numHashFunctions) {
        this(numBits, numHashFunctions, t -> (long) FuncUtils.consistentHash(t), DEFAULT_PREALLOCATE);
    }

    public BloomFilter(long numBits, long numHashFunctions, Function<T, Long> hasher, boolean preallocate) {
        if (numBits <= 0L)
            throw new RuntimeException("number of bits <= 0");
        if (numHashFunctions <= 0L)
            throw new RuntimeException("number of hash functions <= 0");
        if (hasher == null)
            throw new RuntimeException("hasher == null");

        this.numBits = numBits;
        this.hashFunctions = Stream.generate(HF::new).limit(numHashFunctions).toList();
        this.bitSet = new BitSetXXL(numBits, preallocate);
        this.hasher = hasher;
    }

    public BloomFilter(long numBits, long numHashFunctions, Function<T, Long> hasher) {
        this(numBits, numHashFunctions, hasher, DEFAULT_PREALLOCATE);
    }

    public void insert(T item) {
        long hash = hasher.apply(item);
        hashFunctions.stream().mapToLong(hf -> hf.apply(hash)).forEach(bitSet::set);
    }

    public boolean mightContain(T item) {
        long hash = hasher.apply(item);
        return hashFunctions.stream().mapToLong(hf -> hf.apply(hash)).allMatch(bitSet::get);
    }

    public List<? extends LongHashFunction<T>> getHashFunctions() {
        return hashFunctions;
    }

    public BitSetXXL getBitSet() {
        return bitSet;
    }

    public long getNumBits() {
        return numBits;
    }

    public static long numBits(double fpp, double nElements) {
        return (long) Math.ceil(-nElements * Math.log(fpp) / LN_2_SQUARED);
    }

    public static long numBits(long numElements, double falsePositiveProbability, long numHashFunctions) {
        return (long) Math.ceil(-numHashFunctions * numElements / Math.log(1 - Math.pow(falsePositiveProbability, 1d/numHashFunctions)));
    }

    public static double numBitsPerElement(long numBits, long numElements) {
        return (double) numBits / numElements;
    }


    public static long numHashFunctions(double fpp) {
        return (long) Math.ceil(-log2(fpp));
    }

    public static long numHashFunctions(long numBits, long nElements) {
        return (long) Math.ceil((numBits / (double) nElements) * LN_2);
    }

    public static double falsePositiveProb(double numBits, double nElements) {
        return Math.exp(-numBits / nElements * LN_2_SQUARED);
    }

    public static double falsePositiveProb(double numHashFunctions, double numBits, double nElements) {
        return Math.pow(1 - Math.exp(-numHashFunctions * nElements / numBits), numHashFunctions);
    }

    private static double log2(double x) {
        return Math.log(x) / LN_2;
    }

    private class HF implements LongHashFunction<T> {
        private final long a;
        private final long b;

        public HF(long a, long b) {
            this.a = a;
            this.b = b;
        }

        public HF() {
            this(ThreadLocalRandom.current().nextLong(numBits), ThreadLocalRandom.current().nextLong(numBits));
        }

        @Override
        public Long apply(T t) {
            return apply(hasher.apply(t));
        }

        public long apply(long tHashed) {
            return Math.abs(a * tHashed + b) % numBits;
        }

        @Override
        public String toString() {
            return "HF{" +
                    "a=" + a +
                    ", b=" + b +
                    '}';
        }
    }
}
