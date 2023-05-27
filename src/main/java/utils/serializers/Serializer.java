package utils.serializers;

import utils.Coder;
import utils.FuncUtils;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface Serializer<T> extends Coder<T, byte[]> {
    byte[] serialize(T t);
    T deserialize(FileChannel fc, long pos);

    default void serialize(T item, FileChannel fc, long pos) {
        FuncUtils.safeRun(() -> fc.write(ByteBuffer.wrap(serialize(item)), pos));
    }

    T deserialize(byte[] bs);

    @Override
    default byte[] encode(T t) {
        return serialize(t);
    }

    @Override
    default T decode(byte[] bytes) {
        return deserialize(bytes);
    }

}
