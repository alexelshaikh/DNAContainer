package utils;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class BitSetXXL implements Streamable<Long> {
    private static final int MAX_BITS_PER_BITSET_INT = Integer.MAX_VALUE;
    private static final BigInteger MAX_BITS_PER_BITSET = BigInteger.valueOf(MAX_BITS_PER_BITSET_INT);
    public static final BigInteger MAXIMUM_BITS_TOTAL = MAX_BITS_PER_BITSET.multiply(BigInteger.valueOf(Integer.MAX_VALUE));
    private static final boolean DEFAULT_PREALLOCATE = false;

    private final BitSet[] bitSets;
    private final BigInteger numBits;

    public BitSetXXL(long numBits) {
        this(BigInteger.valueOf(numBits), DEFAULT_PREALLOCATE);
    }

    public BitSetXXL(long numBits, boolean preallocate) {
        this(BigInteger.valueOf(numBits), preallocate);
    }

    public BitSetXXL(BigInteger numBits) {
        this(numBits, DEFAULT_PREALLOCATE);
    }

    public BitSetXXL(BigInteger numBits, boolean preallocate) {
        if (numBits.signum() < 0)
            throw new RuntimeException("numBits < 0");
        if (numBits.compareTo(MAXIMUM_BITS_TOTAL) > 0)
            throw new RuntimeException("cannot use more bits than " + MAXIMUM_BITS_TOTAL);

        int numBitSets = numBits.divide(MAX_BITS_PER_BITSET).add(BigInteger.ONE).intValue();

        this.bitSets = new BitSet[numBitSets];
        this.numBits = new BigInteger(numBits.toByteArray());
        for (int i = 0; i < numBitSets; i++) {
            if (preallocate)
                bitSets[i] = new BitSet(MAX_BITS_PER_BITSET_INT);
            else
                bitSets[i] = new BitSet();
        }
    }

    public BigInteger getNumBits() {
        return numBits;
    }

    public int numBitSetsInUse() {
        return bitSets.length;
    }

    public void set(long bitIndex) {
        set(bitIndex, true);
    }

    public void set(BigInteger bitIndex) {
        set(bitIndex, true);
    }

    public void set(long bitIndex, boolean value) {
        int bitSetIndex = whichBitSet(bitIndex);
        bitSets[bitSetIndex].set(whichBitInBitSet(bitIndex), value);
    }

    public void set(BigInteger bitIndex, boolean value) {
        int bitSetIndex = whichBitSet(bitIndex);
        bitSets[bitSetIndex].set(whichBitInBitSet(bitIndex), value);
    }

    public boolean get(long bitIndex) {
        int bitSetIndex = whichBitSet(bitIndex);
        return bitSets[bitSetIndex].get(whichBitInBitSet(bitIndex));
    }

    public boolean get(BigInteger bitIndex) {
        int bitSetIndex = whichBitSet(bitIndex);
        return bitSets[bitSetIndex].get(whichBitInBitSet(bitIndex));
    }

    public LongStream streamAsLongStream() {
        return Arrays.stream(bitSets).flatMapToLong(bs -> bs.stream().mapToLong(__ -> __));
    }

    @Override
    public Stream<Long> stream() {
        return streamAsLongStream().boxed();
    }

    @Override
    public Iterator<Long> iterator() {
        return streamAsLongStream().iterator();
    }

    private int whichBitSet(BigInteger bitIndex) {
        return checkedBitSetIndex(bitIndex.divide(MAX_BITS_PER_BITSET).intValue());
    }

    private int whichBitSet(long bitIndex) {
        return checkedBitSetIndex((int) (bitIndex / MAX_BITS_PER_BITSET_INT));
    }

    private int whichBitInBitSet(BigInteger bitIndex) {
        return checkedBitIndexInBitSet(bitIndex.mod(MAX_BITS_PER_BITSET).intValue());
    }

    private int whichBitInBitSet(long bitIndex) {
        return checkedBitIndexInBitSet((int) (bitIndex % MAX_BITS_PER_BITSET_INT));
    }

    private int checkedBitSetIndex(int bitSetIndex) {
        if (bitSetIndex < 0 || bitSetIndex >= bitSets.length)
            throw new RuntimeException("bitSetIndex < 0 || bitSetIndex > bitSets.length");
        return bitSetIndex;
    }

    private int checkedBitIndexInBitSet(int bitIndexInBitSet) {
        if (bitIndexInBitSet < 0)
            throw new RuntimeException("bitIndexInBitSet < 0");
        return bitIndexInBitSet;
    }

    @Override
    public String toString() {
        return "BitSetXXL{" +
                "numBits=" + numBits +
                '}';
    }
}

