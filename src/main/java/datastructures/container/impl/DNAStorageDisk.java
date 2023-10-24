package datastructures.container.impl;

import core.BaseSequence;
import datastructures.container.Container;
import datastructures.container.DNAContainer;
import datastructures.container.translation.AddressManager;
import utils.*;
import utils.serializers.FixedSizeSerializer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

public class DNAStorageDisk extends DNAContainer.DNAStorage {

    private final Container<Long, Long> diskRouting;
    private final PersistentContainer<AddressedDNA> disk;
    private final ReadWriteLock lock;


    public DNAStorageDisk(AddressManager<Long, BaseSequence> am, Container<Long, Long> diskRouting, int payloadSize, String filePath) {
        super(am);
        if (!am.addressTranslationManager().addressIsFixedSize())
            throw new RuntimeException("variable address size not supported by this DNAStorage");

        this.lock = new ReentrantReadWriteLock();
        int addrSize = am.addressTranslationManager().addressSize();
        int oligoSize = addrSize + payloadSize;
        this.diskRouting = diskRouting;
        this.disk = new PersistentContainer<>(filePath, new FixedSizeSerializer<>() {
            final int serializedSize = 2 + oligoSize / 4;
            @Override
            public int serializedSize() {
                return serializedSize;
            }

            @Override
            public byte[] serialize(AddressedDNA seq) {
                if (seq.length() != oligoSize)
                    throw new RuntimeException("seq cannot be serialized. Reason: seq.length() != serializedSize of this FixedSizedSerializer");

                return Packer.withBytePadding(SeqBitStringConverter.transform(seq.join())).toBytes();
            }

            @Override
            public AddressedDNA deserialize(byte[] bs) {
                BaseSequence oligo = SeqBitStringConverter.transform(Packer.withoutBytePadding(new BitString(bs)));
                return AddressedDNA.of(oligo, addrSize);
            }
        });
    }

    public DNAStorageDisk(AddressManager<Long, BaseSequence> am, int payloadSize, String filePath) {
        this(am, new MapContainer<>(), payloadSize, filePath);
    }

    @Override
    public void put(AddressManager.ManagedAddress<Long, BaseSequence> key, AddressedDNA value) {
        put(key.routed(), value);
    }

    @Override
    public boolean remove(AddressManager.ManagedAddress<Long, BaseSequence> key) {
        return remove(key.routed());
    }

    @Override
    public AddressedDNA get(AddressManager.ManagedAddress<Long, BaseSequence> key) {
        return get(key.routed());
    }

    @Override
    public long size() {
        return disk.size();
    }

    @Override
    public Set<AddressManager.ManagedAddress<Long, BaseSequence>> keys() {
        return readLocked(super::keys);
    }

    @Override
    public boolean contains(AddressManager.ManagedAddress<Long, BaseSequence> key) {
        return readLocked(() -> super.contains(key));
    }

    @Override
    public Iterator<Pair<AddressManager.ManagedAddress<Long, BaseSequence>, AddressedDNA>> iterator() {
        return super.iterator();
    }

    @Override
    public Stream<Pair<AddressManager.ManagedAddress<Long, BaseSequence>, AddressedDNA>> stream() {
        return super.stream();
    }

    @Override
    public Collection<AddressedDNA> values() {
        return readLocked(disk::values);
    }

    @Override
    public boolean isPersistent() {
        return true;
    }

    @Override
    public void put(long key, AddressedDNA value) {
        writeLocked(() -> {
            long diskId = disk.registerId();
            diskRouting.put(key, diskId);
            disk.put(diskId, value);
        });
    }

    @Override
    public boolean remove(long key) {
        throw new UnsupportedOperationException("cannot remove objects from DNAStorageDisk");
    }

    @Override
    public AddressedDNA get(long key) {
        return readLocked(() -> disk.get(diskRouting.get(key)));
    }

    protected  <T> T readLocked(Callable<T> callable) {
        lock.readLock().lock();
        T result = FuncUtils.safeCall(callable);
        lock.readLock().unlock();
        return result;
    }

    protected <T> T writeLocked(Callable<T> callable) {
        lock.writeLock().lock();
        T result = FuncUtils.safeCall(callable);
        lock.writeLock().unlock();
        return result;
    }

    protected void writeLocked(Runnable runnable) {
        writeLocked(Executors.callable(runnable));
    }
}
