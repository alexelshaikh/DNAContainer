package datastructures.container.impl;

import datastructures.container.Container;
import datastructures.container.AddressTranslationManager;
import utils.FuncUtils;
import utils.Pair;
import utils.serializers.FixedSizeSerializer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class PersistentContainer<V> implements Container<Long, V, Long> {
    private long nextFreePos;
    private long size;
    private final FileChannel fc;
    private final FixedSizeSerializer<V> valueSerializer;
    private final long serializedSize;

    public PersistentContainer(String filePath, FixedSizeSerializer<V> valueSerializer) {
        this.nextFreePos = 0L;
        this.fc = FuncUtils.safeCall(() -> FileChannel.open(Path.of(filePath), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE));
        this.valueSerializer = valueSerializer;
        this.serializedSize = valueSerializer.serializedSize();
    }

    private PersistentContainer(String filePath, long size, FixedSizeSerializer<V> valueSerializer) {
        this.serializedSize = valueSerializer.serializedSize();
        this.size = size;
        this.nextFreePos = size * serializedSize;
        this.fc = FuncUtils.safeCall(() -> FileChannel.open(Path.of(filePath), StandardOpenOption.READ, StandardOpenOption.WRITE));
        this.valueSerializer = valueSerializer;
    }

    public static <V> PersistentContainer<V> load(String path, long size, FixedSizeSerializer<V> valueSerializer) {
        return new PersistentContainer<>(path, size, valueSerializer);
    }

    @Override
    public Long put(V value) {
        long id = size;
        putAtPos(nextFreePos, value);
        return id;
    }

    @Override
    public void put(Long key, V value) {
        putAtPos(filePositionFromKey(key), value);
    }

    private void putAtPos(Long pos, V value) {
        synchronized (fc) {
            writeAtPos(pos, value);
            if (pos == nextFreePos)
                nextFreePos += serializedSize;
            size++;
        }
    }

    @Override
    public V get(Long key) {
        synchronized (fc) {
            if (key >= size)
                return null;

            long pos = filePositionFromKey(key);
            return FuncUtils.safeCall(() -> valueSerializer.deserialize(fc, pos));
        }
    }

    private void writeAtPos(long pos, V value) {
        FuncUtils.safeRun(() -> fc.write(ByteBuffer.wrap(valueSerializer.serialize(value)), pos));
    }

    @Override
    public long size() {
        synchronized (fc) {
            return size;
        }
    }

    @Override
    public Long[] registerIds(int n) {
        if (n < 0)
            throw new RuntimeException("cannot register n=" + n + " < 0 Ids");
        synchronized (fc) {
            Long[] array = LongStream.range(size, size + n).boxed().toArray(Long[]::new);
            size += n;
            nextFreePos += serializedSize * n;
            return array;
        }
    }

    @Override
    public Collection<V> values() {
        return LongStream.range(0L, size).mapToObj(this::get).toList();
    }

    @Override
    public Iterator<Pair<Long, V>> iterator() {
        return LongStream.range(0L, size()).mapToObj(id -> new Pair<>(id, get(id))).iterator();
    }

    @Override
    public Set<Long> keys() {
        synchronized (fc) {
            return LongStream.range(0L, size).boxed().collect(Collectors.toSet());
        }
    }

    @Override
    public AddressTranslationManager<Long, Long> getAddressTranslationManager() {
        return AddressTranslationManager.identity();
    }

    protected long filePositionFromKey(Long key) {
        return key * serializedSize;
    }
}
