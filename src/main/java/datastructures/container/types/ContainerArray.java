package datastructures.container.types;

import core.BaseSequence;
import datastructures.container.DNAContainer;
import datastructures.reference.IDNAFedReference;
import datastructures.reference.IDNASketch;
import utils.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ContainerArray<T> extends IDNAFedReference.DNAFedReference<ArrayList<T>, IDNASketch.ContainerIdSketch> implements Streamable<T> {
    private final Coder<T, BaseSequence> coder;

    public ContainerArray(DNAContainer container, IDNASketch.ContainerIdSketch sketch, Coder<T, BaseSequence> coder) {
        super(
                sketch,
                s -> getArrayOligos(container, s.id()).stream().map(AddressedDNA::payload).toArray(BaseSequence[]::new),
                s -> FuncUtils.stream(() -> getArrayIterator(sketch, coder)).collect(Collectors.toCollection(ArrayList::new))
        );
        this.coder = coder;
    }

    @Override
    public Iterator<T> iterator() {
        return getArrayIterator(sketch, coder);
    }

    private BaseSequence getHeaderSeq() {
        return getHeaderSeq(sketch);
    }

    @Override
    public boolean isNullPointer() {
        return getUnpackedHeader() == null;
    }

    private static BaseSequence getHeaderSeq(IDNASketch.ContainerIdSketch sketch) {
        return getHeaderSeq(sketch.container(), sketch.id());
    }

    private static BaseSequence getHeaderSeq(DNAContainer container, long id) {
        return container.get(id);
    }

    @Override
    public BaseSequence[] addresses() {
        return getArrayOligos(sketch.container(), sketch.id()).stream().map(AddressedDNA::address).toArray(BaseSequence[]::new);
    }

    public static List<AddressedDNA> getArrayOligos(DNAContainer container, long id) {
        var header = UnpackedHeader.of(getHeaderSeq(container, id));
        checkOutOfBounds(header, 0, header.length);
        var headerOligos = Arrays.asList(container.getOligos(id));
        List<AddressedDNA> oligos = new ArrayList<>(headerOligos.size() + header.length);
        oligos.addAll(headerOligos);
        for (int i = 0; i < header.length; i++)
            oligos.addAll(Arrays.asList(container.getOligos(header.payloadStartId + i)));

        return oligos;
    }

    public static <T> Iterator<T> getArrayIterator(IDNASketch.ContainerIdSketch sketch, Coder<T, BaseSequence> coder) {
        UnpackedHeader header = UnpackedHeader.of(getHeaderSeq(sketch));
        checkOutOfBounds(header, 0, header.length);
        return getArrayFromStartEndIterator(sketch, coder, header.payloadStartId, 0, header.length);
    }

    public Iterator<T> getFromPosReverseIterator(int startInclusive) {
        UnpackedHeader header = UnpackedHeader.of(getHeaderSeq());
        checkOutOfBounds(header, startInclusive, header.length);
        return getArrayFromStartEndReverseIterator(header.payloadStartId, startInclusive, -1);
    }

    private static <T> Iterator<T> getArrayFromStartEndIterator(IDNASketch.ContainerIdSketch sketch, Coder<T, BaseSequence> coder, long payloadStartId, int startInclusive, int endExclusive) {
        DNAContainer container = sketch.container();
        return new Iterator<>() {
            int pos = startInclusive;
            @Override
            public boolean hasNext() {
                return pos < endExclusive;
            }

            @Override
            public T next() {
                return coder.decode(container.get(payloadStartId + (pos++)));
            }
        };
    }

    private Iterator<T> getArrayFromStartEndReverseIterator(long payloadStartId, int startInclusive, int endExclusive) {
        DNAContainer container = sketch.container();
        return new Iterator<>() {
            int pos = startInclusive;
            @Override
            public boolean hasNext() {
                return pos > endExclusive;
            }

            @Override
            public T next() {
                return coder.decode(container.get(payloadStartId + (pos--)));
            }
        };
    }

    public Iterator<T> getFromPosIterator(int startInclusive, int endExclusive) {
        UnpackedHeader header = UnpackedHeader.of(getHeaderSeq());
        checkOutOfBounds(header, startInclusive, endExclusive);
        return getArrayFromStartEndIterator(sketch, coder, header.payloadStartId, startInclusive, endExclusive);
    }

    public Iterator<T> getFromPosReverseIterator(int startInclusive, int endExclusive) {
        UnpackedHeader header = UnpackedHeader.of(getHeaderSeq());
        checkOutOfBounds(header,endExclusive + 1, startInclusive);
        return getArrayFromStartEndReverseIterator(header.payloadStartId, startInclusive, endExclusive);
    }

    public int length() {
        var header = getUnpackedHeader();
        checkOutOfBounds(header, 0, 1);
        return header.length;
    }

    public T get(int index) {
        var header = getUnpackedHeader();
        checkOutOfBounds(header, index, index + 1);
        return coder.decode(sketch.container().get(header.payloadStartId + index));
    }

    private UnpackedHeader getUnpackedHeader() {
        return UnpackedHeader.of(getHeaderSeq());
    }

    public static <T> ContainerArray<T> putArray(DNAContainer container, Coder<T, BaseSequence> coder, T[] array) {
        return putArray(container, coder, container.registerId(), array, false);
    }

    public static <T> ContainerArray<T> putArray(DNAContainer container, Coder<T, BaseSequence> coder, long rootId, T[] array, boolean parallel) {
        if (array == null || array.length == 0)
            throw new RuntimeException("arrays in DNAContainer must have at least 1 element");

        int arrayLength = array.length;
        long[] ids = container.registerIds(arrayLength);
        long payloadStartId = ids[0];
        int packedArrayLength = arrayLength - 1;
        DNAPacker.LengthBase minLbForArraySize = DNAPacker.LengthBase.fromUnsignedNumber(packedArrayLength);
        DNAPacker.LengthBase minLbForPayloadStartId = DNAPacker.LengthBase.fromUnsignedNumber(packedArrayLength);
        BaseSequence header = BaseSequence.join(
                DNAPacker.pack(packedArrayLength, minLbForArraySize),
                DNAPacker.pack(payloadStartId, minLbForPayloadStartId)
        );
        container.put(rootId, header);
        FuncUtils.stream(IntStream.range(0, ids.length), parallel).forEach(i -> container.put(ids[i], coder.encode(array[i])));
        return new ContainerArray<>(
                container,
                new IDNASketch.ContainerIdSketch(rootId, container),
                coder
        );
    }

    @Override
    public String toString() {
        return getArrayOligos(sketch().container(), sketch.id()).toString();
    }

    private static void checkOutOfBounds(UnpackedHeader header, int startInclusive, int endExclusive) {
        if (header == null)
            throw new RuntimeException("array == null");
        if (startInclusive < 0 || endExclusive < 0)
            throw new RuntimeException("startInclusive < 0 || endExclusive < 0");
        if (header.length < (endExclusive - startInclusive))
            throw new RuntimeException("array.length = " + header.length + " < number of elements in this iterator");
        if (startInclusive >= header.length)
            throw new RuntimeException("index >= array.length");
        if (endExclusive > header.length)
            throw new RuntimeException("index >= array.length");
    }

    private record UnpackedHeader(int length, long payloadStartId) {
        static UnpackedHeader of(BaseSequence headerSeq) {
            if (headerSeq == null)
                return null;
            Number[] unpacked = DNAPacker.unpack(headerSeq, 2, false);
            return new UnpackedHeader(unpacked[0].intValue() + 1, unpacked[1].longValue());
        }
    }
}
