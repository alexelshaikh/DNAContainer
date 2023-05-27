package utils.lsh.minhash;

import core.BaseSequence;
import utils.BitSetXXL;
import java.util.function.Function;

public class BitMinHashLSH<T> extends AbstractMinHashLSH<T> {
    private final BitSetXXL bitArray;
    private final long numBits;

    public BitMinHashLSH(int k, int r, long numBits, Function<T, long[]> kmerFunc) {
        super(k, r, 1, kmerFunc);
        this.bitArray = new BitSetXXL(numBits);
        this.numBits = numBits;

    }

    public static BitMinHashLSH<BaseSequence> newLSHForBaseSequences(int k, int r, long numBits) {
        return new BitMinHashLSH<>(k, r, numBits, seq -> seq.kmers(k).stream().mapToLong(BaseSequence::toBase4).toArray());
    }

    @Override
    public boolean isPresent(T item) {
        return bitArray.get(hash(minHashesPerHashFunction(item)));
    }

    @Override
    protected void insertIntoBand(T element, long[] signature, int bandId) {
        bitArray.set(hash(signature));
    }

    private long hash(long[] signature) {
        return Math.abs(hashSignature(signature) % numBits);
    }
    @Override
    protected void removeFromBand(T element, long[] signature, int bandId) {
        throw new UnsupportedOperationException("cannot remove from a BitMinHashLSH");
    }

    public long getNumBits() {
        return numBits;
    }

    @Override
    public String toString() {
        return "BitMinHashLSH{" +
                "bitArray=" + bitArray +
                '}';
    }
}
