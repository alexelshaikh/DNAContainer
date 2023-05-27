package dnacoders.dnaconvertors;

import core.Base;
import core.BaseSequence;
import utils.BitString;
import utils.Coder;
import utils.SeqBitStringConverter;
import utils.compression.HuffmanCode;
import java.util.stream.Collector;

public class NaiveQuattro implements Coder<String, BaseSequence> {

    private static final int BASE = 4;
    public static final char DIGIT_ZERO   = HuffmanCode.HuffmanSymbol.of(0);
    public static final char DIGIT_ONE    = HuffmanCode.HuffmanSymbol.of(1);
    public static final char DIGIT_TWO    = HuffmanCode.HuffmanSymbol.of(2);
    public static final char DIGIT_THREE  = HuffmanCode.HuffmanSymbol.of(3);

    public static final NaiveQuattro INSTANCE = new NaiveQuattro();

    private final HuffmanCode.Bytes huffCode;

    public NaiveQuattro(HuffmanCode.Bytes huffCode) {
        if (huffCode.getBase() != BASE)
            throw new RuntimeException("Huffman code provided is not of base 4: " + huffCode.getBase());

        this.huffCode = huffCode;
    }

    public NaiveQuattro() {
        this(AbstractRotatingCoder.loadStaticHuffCode(BASE));
    }

    private Base encode(int digit) {
        if (digit == DIGIT_ZERO)
            return Base.A;
        if (digit == DIGIT_ONE)
            return Base.T;
        if (digit == DIGIT_TWO)
            return Base.C;
        if (digit == DIGIT_THREE)
            return Base.G;

        throw new RuntimeException("no base found for digit=" + digit);
    }

    private char decode(Base b) {
        return switch (b) {
            case A -> DIGIT_ZERO;
            case T -> DIGIT_ONE;
            case C -> DIGIT_TWO;
            case G -> DIGIT_THREE;
        };
    }

    @Override
    public BaseSequence encode(String s) {
        return huffCode.encode(s).chars().mapToObj(this::encode).collect(BaseSequence.COLLECTOR_BASE);
    }

    @Override
    public String decode(BaseSequence seq) {
        return huffCode.decodeString(seq.stream().map(this::decode).collect(Collector.of(
                StringBuilder::new,
                StringBuilder::append,
                StringBuilder::append,
                StringBuilder::toString)));
    }

    public BaseSequence encodeDirect(BitString s) {
        if ((s.length() & 1) != 0)
            throw new RuntimeException("BitString.length() % 2 != 0");

        BaseSequence seq = new BaseSequence();
        for (int i = 0; i < s.length(); i += 2)
            seq.append(encode(SeqBitStringConverter.bitsToDigit(s.subString(i, i + 2))));

        return seq;
    }

    public BitString decodeDirect(BaseSequence sequence) {
        BitString bs = new BitString();
        for (int i = 0; i < sequence.length(); i++)
            bs.append(SeqBitStringConverter.digitToBits(decode(sequence.get(i))));

        return bs;
    }
}
