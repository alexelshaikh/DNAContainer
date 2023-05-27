package utils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Packer {
    private Packer() {
    }

    /**
     * The enum for the supported data types.
     */
    public enum Type {
        BYTE("000"),
        SHORT("001"),
        INT("010"),
        LONG("011"),
        FLOAT("100"),
        DOUBLE("101"),
        STRING_8("110"),
        STRING_16("111");

        static final int MAX_SIZE_8_BIT   = 0xFF;
        static final int MAX_SIZE_16_BIT  = 0xFFFF;

        public static final int MIN_SIG_LEN = Arrays.stream(Type.values()).mapToInt(t -> t.signature.length()).min().orElseThrow();
        public static final int MAX_SIG_LEN = Arrays.stream(Type.values()).mapToInt(t -> t.signature.length()).max().orElseThrow();

        private final BitString signature;

        Type(String s) {
            this.signature = new BitString(s);
        }

        public int signatureSize() {
            return signature.length();
        }

        public static int totalSize(BitString bs) {
            return totalSize(bs, 0);
        }

        public static int totalSize(BitString bs, int i) {
            return totalSize(bs, parsePrefix(bs, i), i);
        }

        private static int totalSize(BitString bs, Type t, int i) {
            return t.signature.length() + switch (t) {
                case BYTE -> Byte.SIZE;
                case SHORT -> Short.SIZE;
                case INT -> Integer.SIZE;
                case LONG-> Long.SIZE;
                case FLOAT -> Float.SIZE;
                case DOUBLE -> Double.SIZE;
                case STRING_8 -> Byte.SIZE + bs.toByte(i + STRING_8.signatureSize());
                case STRING_16 -> Short.SIZE + bs.toShort(i + STRING_16.signatureSize());
            };
        }

        /**
         * Parses a signature from a BitString's prefix and returns the corresponding Type.
         * @param bs the BitString containing a Type's signature in its prefix.
         * @return the data type for that signature.
         */
        public static Type parsePrefix(BitString bs) {
            return parsePrefix(bs, 0);
        }

        /**
         * Parses a signature from a BitString at position i and returns the corresponding Type.
         * @param bs the BitString containing a Type's signature at its position i.
         * @return the data type for that signature.
         */
        public static Type parsePrefix(BitString bs, int i) {
            return IntStream.rangeClosed(MIN_SIG_LEN, Math.min(MAX_SIG_LEN, bs.length()))
                    .mapToObj(j -> bs.subString(i, i + j))
                    .map(sig -> Stream.of(Type.values())
                            .filter(t -> t.signature.equals(sig))
                            .findFirst()
                            .orElse(null))
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElseThrow();
        }

        public BitString getSignature() {
            return signature;
        }

        public boolean isNumber() {
            return switch(this) {
                case INT, LONG, FLOAT, DOUBLE, BYTE, SHORT -> true;
                default                                    -> false;
            };
        }

        public static boolean isNumber(BitString sig) {
            return sig != null && !sig.equals(STRING_8.signature) && !sig.equals(STRING_16.signature);
        }

        public boolean isString() {
            return switch(this) {
                case STRING_8, STRING_16 -> true;
                default                  -> false;
            };
        }

        public static boolean isString(BitString sig) {
            return sig != null && (sig.equals(STRING_8.signature) || sig.equals(STRING_16.signature));
        }
    }

    /**
     * Packs a character sequence into a given BitString and Type.
     * @param cs the character sequence to be packed.
     * @param buff the BitString to pack the character sequence into.
     * @param type the data type to pack by.
     */
    public static void pack(CharSequence cs, BitString buff, Type type) {
        String s = cs.toString();
        switch (type) {
            case BYTE, SHORT, INT, LONG, FLOAT, DOUBLE -> packNumber(s, buff, type);
            case STRING_8, STRING_16                   -> packString(s, buff);
            default                                    -> throw new RuntimeException("Type: " + type + " not supported");
        }
    }

    private static void packNumber(String s, BitString buff, Type type) {
        Number n = FuncUtils.parseNumber(s);
        switch (type) {
            case BYTE   -> buff.append(Type.BYTE.signature).append(n.byteValue());
            case SHORT  -> buff.append(Type.SHORT.signature).append(n.shortValue());
            case INT    -> buff.append(Type.INT.signature).append(n.intValue());
            case LONG   -> buff.append(Type.LONG.signature).append(n.longValue());
            case FLOAT  -> buff.append(Type.FLOAT.signature).append(n.floatValue());
            case DOUBLE -> buff.append(Type.DOUBLE.signature).append(n.doubleValue());
            default     -> throw new RuntimeException("Type: " + type + " not Number or not supported");
        }
    }

    /**
     * Packs a String into a given BitString.
     * @param s the String to be packed.
     * @param buff the BitString to be packed into.
     */
    public static void packString(String s, BitString buff) {
        int len = s.length();
        if (len <= Type.MAX_SIZE_8_BIT) {
            buff.append(Type.STRING_8.signature)
                    .append((byte) len)
                    .append(s.getBytes(StandardCharsets.UTF_8));
        }
        else if (len <= Type.MAX_SIZE_16_BIT) {
            buff.append(Type.STRING_16.signature)
                    .append((short) len)
                    .append(s.getBytes(StandardCharsets.UTF_8));
        }
        else {
            throw new RuntimeException("string's length is " + len + " > max allowed length " + Type.MAX_SIZE_16_BIT);
        }
    }

    /**
     * Packs a Number into the smallest available Type a given BitString.
     * @param n the Number to be packed.
     * @param buff the BitString to be packed into.
     */
    public static void packMinimal(Number n, BitString buff) {
        double d = n.doubleValue();
        if (d % 1 == 0) {
            if (d <= Byte.MAX_VALUE && d >= Byte.MIN_VALUE)
                buff.append(Type.BYTE.signature).append(n.byteValue());
            else if (d <= Short.MAX_VALUE && d >= Short.MIN_VALUE)
                buff.append(Type.SHORT.signature).append(n.shortValue());
            else if (d <= Integer.MAX_VALUE && d >= Integer.MIN_VALUE)
                buff.append(Type.INT.signature).append(n.intValue());
            else if (d <= Long.MAX_VALUE && d >= Long.MIN_VALUE)
                buff.append(Type.LONG.signature).append(n.longValue());
            else
                throw new RuntimeException("unknown Number n=" + n + " of class " + n.getClass().getSimpleName() + ". How do we pack those?");
        }
        else {
            if (n instanceof Float)
                buff.append(Type.FLOAT.signature).append(n.floatValue());
            else if (n instanceof Double)
                buff.append(Type.DOUBLE.signature).append(d);
            else
                throw new RuntimeException("unknown Number n=" + n + " of class " + n.getClass().getSimpleName() + ". How do we pack those?");
        }
    }

    /**
     * Adds specific padding to a given BitString such that it contains a multiple of BYTE.SIZE bits. This method allows the original BitString (without padding) to be recovered by <Code>withoutBytePadding(BitString)</Code>.
     * @param inputBs the BitString to be padded.
     * @return a new BitString representing the input BitString with padding.
     */
    public static BitString withBytePadding(BitString inputBs) {
        byte paddingSize = (byte) (Byte.SIZE - inputBs.length() % Byte.SIZE);
        return new BitString()
                .append(paddingSize)
                .append(inputBs)
                .append(false, paddingSize);
    }

    /**
     * Removes the padding added by <Code>withBytePadding(BitString)</Code>.
     * @param bs the BitString with padding.
     * @return a new BitString representing the input BitString without padding.
     */
    public static BitString withoutBytePadding(BitString bs) {
        int numBits = bs.toByte(0);
        return bs.subString(Byte.SIZE, bs.length() - numBits);
    }

    /**
     * Unpacks a String at a given position and Type.
     * @param bitString the BitString with the packed value(s).
     * @param lenIndex the starting position of the packed value.
     * @param strType the Type (a String representing Type) used to unpack.
     * @return the unpacked String.
     */
    public static UnpackingResult<String> unpackString(BitString bitString, int lenIndex, Type strType) {
        int payloadIndex;
        int toIndex;
        switch (strType) {
            case STRING_8 -> {
                payloadIndex = lenIndex + Byte.SIZE;
                toIndex = payloadIndex + bitString.toInt(lenIndex, payloadIndex) * Byte.SIZE;
                return new UnpackingResult<>(new String(bitString.toBytes(payloadIndex, toIndex)), toIndex, Type.STRING_8);
            }
            case STRING_16 -> {
                payloadIndex = lenIndex + Short.SIZE;
                toIndex = payloadIndex + bitString.toInt(lenIndex, payloadIndex) * Byte.SIZE;
                return new UnpackingResult<>(new String(bitString.toBytes(payloadIndex, toIndex)), toIndex, Type.STRING_16);
            }
            default -> throw new RuntimeException("unknown type " + strType);
        }
    }

    /**
     * Unpacks a Number at a given position and Type.
     * @param bitString the BitString with the packed value(s).
     * @param numIndex the starting position of the packed value.
     * @param numType the Type (a Number representing Type) used to unpack.
     * @return the unpacked Number.
     */
    public static UnpackingResult<? extends Number> unpackNumber(BitString bitString, int numIndex, Type numType) {
        int toIndex;
        switch (numType) {
            case BYTE:
                toIndex = numIndex + Byte.SIZE;
                return new UnpackingResult<>(bitString.toByte(numIndex, toIndex), toIndex, Type.BYTE);
            case INT:
                toIndex = numIndex + Integer.SIZE;
                return new UnpackingResult<>(bitString.toInt(numIndex, toIndex), toIndex, Type.INT);
            case SHORT:
                toIndex = numIndex + Short.SIZE;
                return new UnpackingResult<>(bitString.toShort(numIndex, toIndex), toIndex, Type.SHORT);
            case LONG:
                toIndex = numIndex + Long.SIZE;
                return new UnpackingResult<>(bitString.toLong(numIndex, toIndex), toIndex, Type.LONG);
            case FLOAT:
                return new UnpackingResult<>(bitString.toFloat(numIndex), numIndex + Float.SIZE, Type.FLOAT);
            case DOUBLE:
                return new UnpackingResult<>(bitString.toDouble(numIndex), numIndex + Double.SIZE, Type.DOUBLE);
            default:
                throw new RuntimeException("unknown Number type " + numType);
        }
    }

    /**
     * Unpacks a value at a given position and Type.
     * @param bitString the BitString with the packed value(s).
     * @param i the starting position of the packed value.
     * @return the unpacked value.
     */
    public static UnpackingResult<?> unpackAt(BitString bitString, int i) {
        Type type = Type.parsePrefix(bitString, i);
        int payloadIndex = i + type.signatureSize();
        if (type.isNumber())
            return unpackNumber(bitString, payloadIndex, type);
        else
            return unpackString(bitString, payloadIndex, type);
    }

    /**
     * Unpacks all values in a given BitString.
     * @param bitString the BitString with the packed value(s).
     * @return the list of unpacked values.
     */
    public static List<UnpackingResult<?>> unpack(BitString bitString) {
        return unpackLazy(bitString).toList();
    }

    /**
     * Unpacks all values in a given BitString lazily.
     * @param bitString the BitString with the packed value(s).
     * @return the lazy stream of unpacked values.
     */
    public static Stream<UnpackingResult<?>> unpackLazy(BitString bitString) {
        return FuncUtils.stream(() -> new Iterator<>() {
            final int len = bitString.length();
            int fromIndex = 0;

            @Override
            public boolean hasNext() {
                return fromIndex < len;
            }
            @Override
            public UnpackingResult<?> next() {
                UnpackingResult<?> unpacked = FuncUtils.tryOrElse(() -> unpackAt(bitString, fromIndex), () -> {
                    Type t = Type.parsePrefix(bitString, fromIndex);
                    return new UnpackingResult<>(null, fromIndex + Type.totalSize(bitString, t, fromIndex), t);
                });
                fromIndex = unpacked.lastExclusiveIndex;
                return unpacked;
            }
        });
    }

    /**
     * The record holding on to an unpacked value.
     * @param <T> the unpacked value's data type.
     * @param lastExclusiveIndex the last index (inclusive) in the BitString where this value was unpacked.
     */
    public record UnpackingResult<T>(T value, int lastExclusiveIndex, Type type) {

        public boolean isNumber() {
            return type.isNumber();
        }

        public boolean isString() {
            return type.isString();
        }

        public boolean isInt() {
            return type == Type.INT;
        }

        public boolean isShort() {
            return type == Type.SHORT;
        }

        public boolean isLong() {
            return type == Type.LONG;
        }

        public boolean isFloat() {
            return type == Type.FLOAT;
        }

        public boolean isDouble() {
            return type == Type.DOUBLE;
        }

        public boolean isByte() {
            return type == Type.BYTE;
        }

        public T getValue() {
            return value;
        }

        public Type getType() {
            return type;
        }
    }
}

