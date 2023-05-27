package utils;

import core.Base;
import core.BaseSequence;
import dnacoders.dnaconvertors.RotatingQuattro;
import java.util.*;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class DNAPacker {
    private static final RotatingQuattro ROTATOR = RotatingQuattro.INSTANCE;

    private static final Function<BitString, Number> LONG_UNPACK = bs -> bs.toLong(0, bs.length());
    /**
     * The enum for the supported data types.
     */
    public enum LengthBase {
        HALF_BYTE(Byte.SIZE / 2, Base.A, bs -> {
            var b = bs.toByte(0, bs.length());
            return (byte) ((b & 0b0000_1000) != 0 ? ((b & 0b0000_0111) | 0b1000_0000) : b);
        }, LONG_UNPACK),
        BYTE(Byte.SIZE, Base.C, bs -> bs.toByte(0), LONG_UNPACK),
        SHORT(Short.SIZE, Base.T, bs -> bs.toShort(0), LONG_UNPACK),
        INT_32(Integer.SIZE, new BaseSequence(Base.G, Base.A), bs -> bs.toInt(0), LONG_UNPACK),
        INT_64(Long.SIZE, new BaseSequence(Base.G, Base.T), bs -> bs.toLong(0), LONG_UNPACK);

        public static final int MIN_SIG_LEN = Arrays.stream(LengthBase.values()).mapToInt(hd -> hd.signature.length()).min().orElseThrow();
        public static final int MAX_SIG_LEN = Arrays.stream(LengthBase.values()).mapToInt(hd -> hd.signature.length()).max().orElseThrow();
        public final int bitCount;
        public final int payloadSize;
        public final BaseSequence signature;
        private final Function<BitString, ? extends Number> numMapperSigned;
        private final Function<BitString, ? extends Number> numMapperUnsigned;

        LengthBase(int bitCount, BaseSequence sig, Function<BitString, ? extends Number> numMapperSigned, Function<BitString, ? extends Number> numMapperUnsigned) {
            if ((bitCount & 1) != 0)
                throw new RuntimeException("bitCount % 2 != 0");

            this.bitCount = bitCount;
            this.payloadSize = bitCount / 2;
            this.signature = sig;
            this.numMapperSigned = numMapperSigned;
            this.numMapperUnsigned = numMapperUnsigned;
        }

        LengthBase(int bitCount, Base b, Function<BitString, Number> numMapperSigned, Function<BitString, Number> numMapperUnsigned) {
            this(bitCount, new BaseSequence(b), numMapperSigned, numMapperUnsigned);
        }

        /**
         * Returns the LengthBase for the required number of bits of a positive Number.
         * @param bitCount the number of bits.
         * @return the LengthBase.
         */
        public static LengthBase parseSignedNumber(int bitCount) {
            if (bitCount < HALF_BYTE.bitCount)
                return HALF_BYTE;
            else if (bitCount < BYTE.bitCount)
                return BYTE;
            else if (bitCount < SHORT.bitCount)
                return SHORT;
            else if (bitCount < INT_32.bitCount)
                return INT_32;
            else if (bitCount < INT_64.bitCount)
                return INT_64;

            throw new RuntimeException("BitCount was invalid: " + bitCount);
        }

        /**
         * Returns the LengthBase for the required number of bits of a negative Number.
         * @param bitCount the number of bits.
         * @return the LengthBase.
         */
        public static LengthBase parseUnsignedNumber(int bitCount) {
            if (bitCount <= HALF_BYTE.bitCount)
                return HALF_BYTE;
            else if (bitCount <= BYTE.bitCount)
                return BYTE;
            else if (bitCount <= SHORT.bitCount)
                return SHORT;
            else if (bitCount <= INT_32.bitCount)
                return INT_32;
            else if (bitCount <= INT_64.bitCount)
                return INT_64;

            throw new RuntimeException("BitCount was invalid: " + bitCount);
        }


        /**
         * Returns the LengthBase required to store a given number.
         * @param n the number.
         * @return the LengthBase.
         */
        public static LengthBase fromNumber(long n, boolean signed) {
            BitString bs = new BitString().append(n, false);
            return signed ? parseSignedNumber(bs.length()) : parseUnsignedNumber(bs.length());
        }

        /**
         * Returns the LengthBase required to store a given unsigned number.
         * @param n the number.
         * @return the LengthBase.
         */
        public static LengthBase fromUnsignedNumber(long n) {
            return fromNumber(n, false);
        }

        /**
         * Returns the LengthBase required to store a given signed number.
         * @param n the number.
         * @return the LengthBase.
         */
        public static LengthBase fromSignedNumber(long n) {
            return fromNumber(n, true);
        }

        /**
         * Returns the smallest Number's subclass representing the given int.
         * @param n the number.
         * @return the smallest subclass of Number possible for the input.
         */
        public static Number minimize(long n) {
            if (n < 0) {
                long p = -n;
                if (p <= 0b01111111)
                    return (byte) (~p + 1);
                if (p <= 0b01111111_11111111)
                    return (short) (~p + 1);
                if (p <= 0b0111111_11111111_11111111_11111111)
                    return (int) (~p + 1);
            }
            else {
                if (n <= 0x7F)
                    return (byte) n;
                if (n <= 0x7FFF)
                    return (short) n;
                if (n <= 0x7FFF_FFFF)
                    return (int) n;
            }
            return n;
        }

        /**
         * Decodes the LengthBase from a DNA sequence's prefix.
         * @param seq the DNA sequence.
         * @return the LengthBase.
         */
        public static LengthBase parsePrefix(BaseSequence seq) {
            return IntStream.rangeClosed(MIN_SIG_LEN, Math.min(MAX_SIG_LEN, seq.length()))
                    .mapToObj(i -> seq.window(0, i))
                    .map(sig -> Stream.of(LengthBase.values())
                            .filter(hd -> hd.signature.equals(sig))
                            .findFirst()
                            .orElse(null))
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElseThrow();
        }

        /**
         * Unpacks the first packed number in a given DNA sequence.
         * @param seq the DNA sequence.
         * @param signed true, to interpret the highest bit of the used LengthBase as sign, and false to unpack the value as unsigned.
         * @return the decoded number.
         */
        public Number unpackSingle(BaseSequence seq, boolean signed) {
            return signed ? unpackSingle(seq) : unpackSingleUnsignedToLong(seq);
        }

        /**
         * Unpacks the first packed number in a given DNA sequence.
         * @param seq the DNA sequence.
         * @return the decoded number.
         */
        public Number unpackSingle(BaseSequence seq) {
            BaseSequence encodedValue = seq.window(getSignatureSize(), totalSize());
            BitString decodedBases = ROTATOR.decodeDirect(encodedValue);
            return numMapperSigned.apply(decodedBases);
        }

        /**
         * Unpacks the first packed number in a given DNA sequence as long. Returns a positive number if the highest bit is 0.
         * @param seq the DNA sequence.
         * @return the decoded number.
         */
        public Number unpackSingleUnsignedToLong(BaseSequence seq) {
            BaseSequence encodedValue = seq.window(getSignatureSize(), totalSize());
            BitString decodedBases = ROTATOR.decodeDirect(encodedValue);
            return numMapperUnsigned.apply(decodedBases);
        }

        public int getSignatureSize() {
            return signature.length();
        }
        public int getPayloadSize() {
            return payloadSize;
        }
        public int totalSize() {
            return signature.length() + payloadSize;
        }
        public BaseSequence getSignature() {
            return signature;
        }
    }


    /**
     * Packs the given Numbers to the supplied DNA sequence.
     * @param seq the DNA sequence.
     * @param values the numbers to be packed.
     * @return the DNA sequence with packed numbers to its end.
     */
    public static BaseSequence pack(BaseSequence seq, Number... values) {
        for (var value : values)
            pack(seq, value);

        return seq;
    }

    /**
     * Packs the given Numbers to the supplied DNA sequence.
     * @param seq the DNA sequence.
     * @param values the numbers to be packed.
     * @return the DNA sequence with packed numbers to its end.
     */
    public static BaseSequence pack(BaseSequence seq, List<? extends Number> values) {
        for (var value : values)
            pack(seq, value);

        return seq;
    }

    /**
     * Packs the given unsigned Numbers to the supplied DNA sequence.
     * @param seq the DNA sequence.
     * @param values the numbers to be packed.
     * @return the DNA sequence with packed numbers to its end.
     */
    public static BaseSequence packUnsigned(BaseSequence seq, List<? extends Number> values) {
        for (var value : values)
            packUnsigned(seq, value);

        return seq;
    }

    /**
     * Packs the given Numbers to a new DNA sequence.
     * @param values the numbers to be packed.
     * @return the DNA sequence with packed numbers to its end.
     */
    public static BaseSequence pack(Number... values) {
        return pack(new BaseSequence(), values);
    }

    /**
     * Packs the given Numbers to a new DNA sequence.
     * @param values the numbers to be packed.
     * @return the DNA sequence with packed numbers to its end.
     */
    public static BaseSequence pack(List<? extends Number> values) {
        return pack(new BaseSequence(), values);
    }

    public static BaseSequence pack(BaseSequence seq, Number n) {
        BitString bits = new BitString();
        appendMinimal(bits, n, false);
        LengthBase header = LengthBase.parseSignedNumber(bits.length());
        return pack(seq, n, header);
    }

    /**
     * Packs the given unsigned Numbers to a new DNA sequence.
     * @param values the numbers to be packed.
     * @return the DNA sequence with packed numbers to its end.
     */
    public static BaseSequence packUnsigned(List<? extends Number> values) {
        return packUnsigned(new BaseSequence(), values);
    }

    public static void packUnsigned(BaseSequence seq, Number n) {
        BitString bits = new BitString();
        appendMinimal(bits, n, false);
        LengthBase header = LengthBase.parseUnsignedNumber(bits.length());
        pack(seq, n, header);
    }

    /**
     * Packs the given number to the supplied DNA sequence as specified by a LengthBase.
     * @param seq the DNA sequence.
     * @param n the number to be packed.
     * @param lengthBase the LengthBase used to pack the number.
     * @return the DNA sequence with the packed number to its end.
     */
    public static BaseSequence pack(BaseSequence seq, Number n, LengthBase lengthBase) {
        BitString newBits = new BitString();
        appendAndFillTo(newBits, n, lengthBase.bitCount);
        seq.append(lengthBase.signature);
        seq.append(ROTATOR.encodeDirect(newBits));

        return seq;
    }

    /**
     * Packs the given number to a new DNA sequence as specified by a LengthBase.
     * @param n the number to be packed.
     * @param lengthBase the LengthBase used to pack the number.
     * @return the DNA sequence with the packed number.
     */
    public static BaseSequence pack(Number n, LengthBase lengthBase) {
        return pack(new BaseSequence(), n, lengthBase);
    }

    /**
     * Packs the given number to a new DNA sequence. This method packs the number with as few DNA bases as possible.
     * @param n the number to be packed.
     * @return the DNA sequence with the packed number.
     */
    public static BaseSequence pack(Number n) {
        return pack(new BaseSequence(), n);
    }

    public static void appendMinimal(BitString bs, Number n, boolean fill) {
        if (n instanceof Byte)
            bs.append(n.byteValue(), fill);
        else if (n instanceof Short)
            bs.append(n.shortValue(), fill);
        else if (n instanceof Integer)
            bs.append(n.intValue(), fill);
        else if (n instanceof Long)
            bs.append(n.longValue(), fill);
        else
            throw new RuntimeException("unsupported number: " + n + " of type: " + n.getClass().getSimpleName());
    }

    /**
     * Appends the given number to the BitString.
     * @param bitString the BitString.
     * @param n the number that is appended to the BitString
     * @param fillToNumBits the number of bits the appended number should have. Fills in leading zeros to achieve fillToNumBits bits in total.
     */
    public static void appendAndFillTo(BitString bitString, Number n, int fillToNumBits) {
        BitString bs = new BitString();
        if (n instanceof Byte)
            bs.append(n.byteValue(), false);
        else if (n instanceof Short)
            bs.append(n.shortValue(), false);
        else if (n instanceof Integer)
            bs.append(n.intValue(), false);
        else if (n instanceof Long)
            bs.append(n.longValue(), false);
        else
            throw new RuntimeException("Floats and Doubles have a static length. This method only supports ints, bytes, shorts, and longs.");

        if(bs.length() > fillToNumBits)
            throw new RuntimeException("number " + n + " requires " + bs.length() + " bits > fillToNumBits(" + fillToNumBits + ")");

        bitString.append(false, fillToNumBits - bs.length()).append(bs);
    }

    /**
     * Unpacks packed values in a given DNA sequence.
     * @param sequence the DNA sequence that contains packed values.
     * @param count the number of packed values to unpack.
     * @return the array of unpacked values.
     */
    public static Number[] unpack(BaseSequence sequence, int count) {
        return unpack(sequence, count, false);
    }

    /**
     * Unpacks packed values in a given DNA sequence.
     * @param sequence the DNA sequence that contains packed values.
     * @param count the number of packed values to unpack.
     * @param signed true, to unpack the values, interpreting the respective highest bit for the used LengthBase as sign, and false otherwise.
     * @return the array of unpacked values.
     */
    public static Number[] unpack(BaseSequence sequence, int count, boolean signed) {
        Number[] vals = new Number[count];
        var index = 0;
        BaseSequence window;
        LengthBase lb;
        for (int i = 0; i < count; i++) {
            window = sequence.window(index);
            lb = LengthBase.parsePrefix(window);
            vals[i] = lb.unpackSingle(window, signed);
            index += lb.totalSize();
        }
        return vals;
    }

    /**
     * Calculates the total number of DNA bases used to pack packedValueCount values.
     * @param sequence the DNA sequence.
     * @param packedValueCount the number of packed values.
     * @return the total number of DNA bases.
     */
    public static int getPackedSize(BaseSequence sequence, int packedValueCount) {
        var index = 0;
        for (int i = 0; i < packedValueCount; i++)
            index += LengthBase.parsePrefix(sequence.window(index)).totalSize();

        return index;
    }

    /**
     * Unpacks a single value from a given DNA sequence.
     * @param seq the DNA sequence.
     * @return the unpacked value.
     */
    public static Number unpack(BaseSequence seq) {
        return unpack(seq, true);
    }

    /**
     * Unpacks a single value from a given DNA sequence.
     * @param seq the DNA sequence.
     * @param signed true, to unpack the value, interpreting the highest bit for the used LengthBase as sign, and false otherwise.
     * @return the unpacked value.
     */
    public static Number unpack(BaseSequence seq, boolean signed) {
        return LengthBase.parsePrefix(seq).unpackSingle(seq, signed);
    }

    /**
     * Unpacks all values from a given DNA sequence.
     * @param seq the DNA sequence.
     * @return the unpacked values.
     */
    public static List<Number> unpackAll(BaseSequence seq) {
        return unpackAll(seq, true);
    }


    /**
     * Unpacks all values from a given DNA sequence.
     * @param seq the DNA sequence.
     * @param signed true, to unpack all values, interpreting the respective highest bit for the used LengthBase as sign, and false otherwise.
     * @return the unpacked values.
     */
    public static List<Number> unpackAll(BaseSequence seq, boolean signed) {
        LengthBase lb;
        List<Number> result = new ArrayList<>();
        while(seq.length() > 0) {
            BaseSequence finalSeq = seq;
            lb = FuncUtils.tryOrElse(() -> LengthBase.parsePrefix(finalSeq), () -> null);
            if (lb == null)
                return result;
            result.add(lb.unpackSingle(seq, signed));
            seq = seq.window(lb.totalSize());
        }
        return result;
    }

    public static Stream<Number> unpackAllStream(BaseSequence seq, boolean signed) {
        return FuncUtils.stream(() -> new Iterator<>() {
            BaseSequence s = seq;
            LengthBase lb;
            @Override
            public boolean hasNext() {
                return s.length() > 0;
            }

            @Override
            public Number next() {
                lb = LengthBase.parsePrefix(s);
                Number n = lb.unpackSingle(s, signed);
                s = s.window(lb.totalSize());
                return n;
            }
        });
    }

    public static Stream<Number> unpackAllStream(BaseSequence seq) {
       return unpackAllStream(seq, true);
    }
}
