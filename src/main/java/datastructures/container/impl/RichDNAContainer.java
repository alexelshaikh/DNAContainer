package datastructures.container.impl;

import core.BaseSequence;
import datastructures.container.DNAContainer;
import datastructures.container.translation.AddressManager;
import datastructures.container.types.ContainerArray;
import datastructures.container.types.ContainerList;
import datastructures.reference.IDNAFedReference;
import datastructures.reference.IDNASketch;
import utils.AddressedDNA;
import utils.Coder;
import java.util.*;

public class RichDNAContainer<T> implements DNAContainer {

    private final Coder<T, BaseSequence> coder;
    private final DNAContainer container;

    public RichDNAContainer(DNAContainer container, Coder<T, BaseSequence> coder) {
        this.container = container;
        this.coder = coder;
    }

    public DNAContainer getContainer() {
        return container;
    }

    public Coder<T, BaseSequence> getCoder() {
        return coder;
    }

    @Override
    public long size() {
        return container.size();
    }

    @Override
    public Set<Long> keys() {
        return container.keys();
    }

    @Override
    public void put(Long key, BaseSequence value) {
        container.put(key, value);
    }

    @Override
    public BaseSequence get(Long key) {
        return container.get(key);
    }

    @Override
    public DNAStorage getOligoStore() {
        return container.getOligoStore();
    }

    @Override
    public BaseSequence assembleFromOligos(AddressedDNA[] oligos) {
        return container.assembleFromOligos(oligos);
    }

    @Override
    public AddressManager<Long, BaseSequence> getAddressManager() {
        return container.getAddressManager();
    }

    @Override
    public long put(BaseSequence seq) {
        return container.put(seq);
    }

    @Override
    public long registerId() {
        return container.registerId();
    }

    @Override
    public long[] registerIds(int n) {
        return container.registerIds(n);
    }

    @Override
    public int getAddressSize() {
        return container.getAddressSize();
    }

    @Override
    public int getPayloadSize() {
        return container.getPayloadSize();
    }

    @Override
    public AddressedDNA[] getOligos(long id) {
        return container.getOligos(id);
    }

    public IDNAFedReference.DNAFedReference<T, IDNASketch.ContainerIdSketch> putReference(T reference) {
        return putReference(container.registerId(), reference);
    }

    public IDNAFedReference.DNAFedReference<T, IDNASketch.ContainerIdSketch> putReference(long id, T reference) {
        container.put(id, coder.encode(reference));
        return new IDNAFedReference.DNAFedReference<>(
                new IDNASketch.ContainerIdSketch(id, container),
                __ -> container.getPayloads(id),
                __ -> coder.decode(container.get(id))
        );
    }

    public IDNAFedReference.DNAFedReference<T, IDNASketch.ContainerIdSketch> getReference(IDNASketch.ContainerIdSketch sketch) {
        return getReference(sketch.id());
    }

    public IDNAFedReference.DNAFedReference<T, IDNASketch.ContainerIdSketch> getReference(long id) {
        return new IDNAFedReference.DNAFedReference<>(
                new IDNASketch.ContainerIdSketch(id, container),
                __ -> container.getPayloads(id),
                __ -> coder.decode(container.get(id))
        );
    }

    public ContainerList<T> putList(List<T> seqs) {
        return ContainerList.putList(container, coder, seqs);
    }

    public ContainerList<T> putList(long rootId, List<T> list) {
        return ContainerList.putList(container, coder, rootId, list);
    }

    public ContainerList<T> putEmptyList(long rootId) {
        return ContainerList.putEmptyList(container, coder, rootId);
    }

    public ContainerList<T> getList(IDNASketch.ContainerIdSketch sketch) {
        return getList(sketch.id());
    }

    public ContainerList<T> getList(long rootId) {
        return new ContainerList<>(
                container,
                new IDNASketch.ContainerIdSketch(rootId, container),
                coder
        );
    }

    public ContainerArray<T> putArray(T[] array, boolean parallel) {
        return putArray(container.registerId(), array, parallel);
    }

    public ContainerArray<T> putArray(T[] array) {
        return putArray(container.registerId(), array, false);
    }

    public ContainerArray<T> putArray(long rootId, T[] array) {
        return putArray(rootId, array, false);
    }

    public ContainerArray<T> putArray(long rootId, T[] array, boolean parallel) {
        return ContainerArray.putArray(container, coder, rootId, array, parallel);
    }

    public ContainerArray<T> getArray(IDNASketch.ContainerIdSketch sketch) {
        return getArray(sketch.id());
    }

    public ContainerArray<T> getArray(long rootId) {
        return new ContainerArray<>(
                container,
                new IDNASketch.ContainerIdSketch(rootId, container),
                coder
        );
    }
}
