package datastructures.container.impl;

import datastructures.container.Container;
import utils.FuncUtils;
import utils.serializers.FixedSizeSerializer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class PersistentContainer<V> extends Container.LinearLongContainer<V> {
    private long nextFreePos;
    private final FileChannel fc;
    private final FixedSizeSerializer<V> valueSerializer;
    private final long serializedSize;
    private long size;
    private final String filePath;

    public PersistentContainer(String filePath, FixedSizeSerializer<V> valueSerializer) {
        this.nextFreePos = 0L;
        this.fc = FuncUtils.safeCall(() -> FileChannel.open(Path.of(filePath), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE));
        this.valueSerializer = valueSerializer;
        this.serializedSize = valueSerializer.serializedSize();
        this.size = 0L;
        this.filePath = filePath;
    }

    public static <V> PersistentContainer<V> load(String filePath, FixedSizeSerializer<V> valueSerializer) {
        PersistentContainer<V> pc = new PersistentContainer<>(filePath, valueSerializer);
        pc.nextFreePos = FuncUtils.safeCall(pc.fc::size);
        if (pc.nextFreePos % valueSerializer.serializedSize() != 0L)
            throw new RuntimeException("failed loading persistent container from " + filePath);

        pc.size = pc.nextFreePos / pc.serializedSize;
        pc.gen.sync(pc.size - 1);
        return pc;
    }

    public String getFilePath() {
        return filePath;
    }

    @Override
    public void put(Long key, V value) {
        if (key > size)
            throw new RuntimeException("key > size");

        long pos = filePositionFromKey(key);
        FuncUtils.safeRun(() -> fc.write(ByteBuffer.wrap(valueSerializer.serialize(value)), pos));
        if (pos == nextFreePos) {
            nextFreePos += serializedSize;
            size++;
            if (gen.getCurrentNextFreeId() == key)
                gen.advance();
        }
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public boolean isPersistent() {
        return true;
    }

    @Override
    public boolean remove(Long key) {
        throw new UnsupportedOperationException("cannot remove objects from a persistent container");
    }

    @Override
    public V get(Long key) {
        if (key >= size())
            return null;

        long pos = filePositionFromKey(key);
        return FuncUtils.safeCall(() -> valueSerializer.deserialize(fc, pos));
    }

    protected long filePositionFromKey(Long key) {
        return key * serializedSize;
    }
}
