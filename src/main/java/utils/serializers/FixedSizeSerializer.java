package utils.serializers;

import utils.Coder;
import utils.FuncUtils;
import utils.Pair;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Stream;

public interface FixedSizeSerializer<T> extends Serializer<T> {

    FixedSizeSerializer<Integer> INT    = FixedSizeSerializer.of(i -> ByteBuffer.allocate(Integer.BYTES).putInt(i).array(), bs -> ByteBuffer.wrap(bs).getInt(), Integer.BYTES);
    FixedSizeSerializer<Long>    LONG   = FixedSizeSerializer.of(l -> ByteBuffer.allocate(Long.BYTES).putLong(l).array(), bs -> ByteBuffer.wrap(bs).getLong(), Long.BYTES);
    FixedSizeSerializer<Float>   FLOAT  = FixedSizeSerializer.of(f -> ByteBuffer.allocate(Float.BYTES).putFloat(f).array(), bs -> ByteBuffer.wrap(bs).getFloat(), Float.BYTES);
    FixedSizeSerializer<Double>  DOUBLE = FixedSizeSerializer.of(d -> ByteBuffer.allocate(Double.BYTES).putDouble(d).array(), bs -> ByteBuffer.wrap(bs).getDouble(), Double.BYTES);

    int serializedSize();

    @Override
    default T deserialize(FileChannel fc, long pos) {
        ByteBuffer bf = ByteBuffer.allocate(serializedSize());
        FuncUtils.safeRun(() -> fc.read(bf, pos));
        return FuncUtils.safeCall(() -> deserialize(bf.array()));
    }


    static <T> FixedSizeSerializer<T> of(Function<T, byte[]> serializer, Function<byte[], T> deserializer, int serializedSize) {
        return of(Coder.of(serializer, deserializer), serializedSize);
    }

    static <T> FixedSizeSerializer<T> of(Coder<T, byte[]> coder, int serializedSize) {
        return new FixedSizeSerializer<>() {
            @Override
            public int serializedSize() {
                return serializedSize;
            }
            @Override
            public byte[] serialize(T t) {
                return coder.encode(t);
            }
            @Override
            public T deserialize(byte[] bs) {
                return coder.decode(bs);
            }
        };
    }

    static <T1, T2> FixedSizeSerializer<Pair<T1, T2>> fuse(FixedSizeSerializer<T1> s1, FixedSizeSerializer<T2> s2) {
        return fuse(s1, s2, Pair::new);
    }

    static <T1, T2, T extends Pair<T1, T2>> FixedSizeSerializer<T> fuse(FixedSizeSerializer<T1> s1, FixedSizeSerializer<T2> s2, BiFunction<T1, T2, T> objectConstructor) {
        return new FixedSizeSerializer<>() {
            @Override
            public int serializedSize() {
                return s1.serializedSize() + s2.serializedSize();
            }

            @Override
            public byte[] serialize(T p) {
                return ByteBuffer.allocate(serializedSize())
                        .put(s1.serialize(p.getT1()))
                        .put(s2.serialize(p.getT2()))
                        .array();
            }

            @Override
            public T deserialize(byte[] bs) {
                byte[] slice1 = Arrays.copyOfRange(bs, 0, s1.serializedSize());
                byte[] slice2 = Arrays.copyOfRange(bs, s1.serializedSize(), serializedSize());
                return objectConstructor.apply(s1.deserialize(slice1), s2.deserialize(slice2));
            }
        };
    }

    static <T> FixedSizeSerializer<T[]> fixedSizeArraySerializer(FixedSizeSerializer<T> s, int n, IntFunction<T[]> arraySupplier) {
        return new FixedSizeSerializer<>() {
            @Override
            public int serializedSize() {
                return n * s.serializedSize();
            }

            @Override
            public byte[] serialize(T[] ts) {
                if (ts.length != n)
                    throw new RuntimeException("array.length: " + ts.length + " != " + n);

                ByteBuffer bf = ByteBuffer.allocate(serializedSize());
                Arrays.stream(ts).forEach(t -> bf.put(s.serialize(t)));
                return bf.array();
            }

            @Override
            public T[] deserialize(byte[] bs) {
                int size = s.serializedSize();
                if (size * n != bs.length)
                    throw new RuntimeException("bytes do not correspond to this instance of Serializer");
                int start = 0;
                int end = size;
                Stream.Builder<T> sb = Stream.builder();
                for (int i = 0; i < n; i++) {
                    sb.add(s.deserialize(Arrays.copyOfRange(bs, start, end)));
                    start = end;
                    end += size;
                }
                return sb.build().toArray(arraySupplier);
            }
        };
    }
}
