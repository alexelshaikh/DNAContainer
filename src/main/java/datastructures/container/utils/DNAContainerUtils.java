package datastructures.container.utils;

import core.BaseSequence;
import datastructures.container.DNAContainer;
import datastructures.container.translation.RoutingManager;
import utils.AddressedDNA;
import utils.DNAPacker;
import utils.FuncUtils;
import utils.Pair;
import java.util.*;
import java.util.stream.IntStream;

public class DNAContainerUtils {

    public static long putArray(DNAContainer container, BaseSequence... seqs) {
        return putArray(container, false, seqs);
    }

    public static long putArrayParallel(DNAContainer container, BaseSequence... seqs) {
        return putArray(container, true, seqs);
    }

    public static long putArray(DNAContainer container, boolean parallel, BaseSequence... seqs) {
        int arrayLength = seqs.length;
        long[] ids = container.registerIds(arrayLength + 1);
        long headerId = ids[0];
        int packedArrayLength = arrayLength - 1;
        DNAPacker.LengthBase lb = DNAPacker.LengthBase.fromUnsignedNumber(packedArrayLength);
        BaseSequence header = DNAPacker.pack(packedArrayLength, lb);
        container.put(headerId, header);
        FuncUtils.stream(IntStream.range(1, ids.length), parallel).forEach(i -> container.put(ids[i], seqs[i - 1]));
        return headerId;
    }

    public static List<AddressedDNA> getArrayOligos(DNAContainer container, long id) {
        BaseSequence header = container.get(id);
        var headerOligos = Arrays.asList(container.getOligos(id));
        if (header == null)
            return Collections.emptyList();
        int arrayLength = DNAPacker.unpack(header, false).intValue() + 1;
        List<AddressedDNA> oligos = new ArrayList<>(headerOligos.size() + arrayLength);
        oligos.addAll(headerOligos);
        long payloadStartId = id + 1;
        for (int i = 0; i < arrayLength; i++)
            oligos.addAll(Arrays.asList(container.getOligos(payloadStartId + i)));

        return oligos;
    }

    public static BaseSequence[] getArray(DNAContainer container, long id) {
        BaseSequence header = container.get(id);
        if (header == null)
            return null;
        int arrayLength = DNAPacker.unpack(header, false).intValue() + 1;
        BaseSequence[] array = new BaseSequence[arrayLength];
        long payloadStartId = id + 1;
        for (int i = 0; i < array.length; i++)
            array[i] = container.get(payloadStartId + i);

        return array;
    }

    public static Iterator<BaseSequence> getArrayIterator(DNAContainer container, long id) {
        return getArrayFromPosIterator(container, id, 0);
    }

    public static Iterator<BaseSequence> getArrayFromPosIterator(DNAContainer container, long id, int start) {
        BaseSequence header = container.get(id);
        if (header == null)
            return null;

        var arrayLength = DNAPacker.unpack(header, false).intValue() + 1;
        return getArrayFromStartEndIterator(container, id, start, arrayLength);
    }


    public static Iterator<BaseSequence> getArrayFromPosReverseIterator(DNAContainer container, long id, int startInclusive) {
        BaseSequence header = container.get(id);
        if (header == null)
            return null;

        return getArrayFromStartEndReverseIterator(container, id, startInclusive, -1);
    }

    private static Iterator<BaseSequence> getArrayFromStartEndIterator(DNAContainer container, long id, int startInclusive, int endExclusive) {
        long payloadStartId = id + 1;

        return new Iterator<>() {
            int pos = startInclusive;
            @Override
            public boolean hasNext() {
                return pos < endExclusive;
            }

            @Override
            public BaseSequence next() {
                return container.get(payloadStartId + (pos++));
            }
        };
    }

    private static Iterator<BaseSequence> getArrayFromStartEndReverseIterator(DNAContainer container, long id, int startInclusive, int endExclusive) {
        long payloadStartId = id + 1;

        return new Iterator<>() {
            int pos = startInclusive;
            @Override
            public boolean hasNext() {
                return pos > endExclusive;
            }

            @Override
            public BaseSequence next() {
                return container.get(payloadStartId + (pos--));
            }
        };
    }

    public static Iterator<BaseSequence> getArrayFromPosIterator(DNAContainer container, long id, int startInclusive, int endExclusive) {
        BaseSequence header = container.get(id);
        if (header == null)
            return null;

        return getArrayFromStartEndIterator(container, id, startInclusive, endExclusive);
    }

    public static Iterator<BaseSequence> getArrayFromPosReverseIterator(DNAContainer container, long id, int startInclusive, int endExclusive) {
        BaseSequence header = container.get(id);
        if (header == null)
            return null;

        return getArrayFromStartEndReverseIterator(container, id, startInclusive, endExclusive);
    }

    public static int getArrayLength(DNAContainer container, long id) {
        BaseSequence header = container.get(id);
        if (header == null)
            return -1;

        return DNAPacker.unpack(header, false).intValue() + 1;
    }

    public static BaseSequence getArrayPos(DNAContainer container, long id, int pos) {
        if (pos < 0)
            throw new RuntimeException("pos: " + pos + " < 0 is out of bounds");
        BaseSequence header = container.get(id);
        if (header == null)
            return null;
        var arrayLength = DNAPacker.unpack(header, false).intValue() + 1;
        if (pos >= arrayLength)
            throw new RuntimeException("pos: " + pos + " >= " + arrayLength + " is out of bounds");

        return getArrayPosUnchecked(container, id, pos);
    }

    public static BaseSequence getArrayPosUnchecked(DNAContainer container, long id, int pos) {
        return container.get(id + 1 + pos);
    }

    public static long putList(DNAContainer container, List<BaseSequence> seqs) {
        if (seqs == null || seqs.isEmpty())
            return putEmptyList(container);

        long[] ids = container.registerIds(seqs.size() + 1);
        for (int i = 0; i < seqs.size(); i++)
            container.put(ids[i], BaseSequence.join(DNAPacker.pack(ids[i + 1], DNAPacker.LengthBase.INT_64), seqs.get(i)));

        return ids[0];
    }

    public static long putList(DNAContainer container, BaseSequence... seqs) {
        if (seqs == null || seqs.length == 0)
            return putEmptyList(container);

        return putList(container, Arrays.asList(seqs));
    }

    public static long putEmptyList(DNAContainer container) {
        long[] ids = container.registerIds(2);
        container.put(ids[0], DNAPacker.pack(ids[1], DNAPacker.LengthBase.INT_64));
        return ids[0];
    }

    public static List<BaseSequence> getList(DNAContainer container, long id) {
        return FuncUtils.stream(() -> getListIterator(container, id)).toList();
    }

    public static Iterator<BaseSequence> getListIterator(DNAContainer container, long listId) {
        return FuncUtils.stream(() -> getRichListIterator(container, listId)).map(Pair::getT1).iterator();
    }

    public static Iterator<Pair<BaseSequence, Long>> getRichListIterator(DNAContainer container, long listId) {
        BaseSequence seq = container.get(listId);
        if (seq == null)
            return Collections.emptyIterator();

        if (seq.length() == DNAPacker.LengthBase.INT_64.totalSize()) {
            listId = DNAPacker.unpack(seq, false).longValue();
            seq = container.get(listId);
        }

        BaseSequence seqFinal = seq;
        final long nextId = listId;

        return new Iterator<>() {
            BaseSequence s = seqFinal;
            long id = nextId;

            @Override
            public boolean hasNext() {
                return s != null && s.length() > DNAPacker.LengthBase.INT_64.totalSize();
            }

            @Override
            public Pair<BaseSequence, Long> next() {
                if (!hasNext())
                    throw new NoSuchElementException("Iterator exhausted! No more elements to return.");
                BaseSequence r = s.window(DNAPacker.LengthBase.INT_64.totalSize());
                id = DNAPacker.unpack(s, false).longValue();
                s = container.get(id);
                return new Pair<>(r, id);
            }
        };
    }

    public static List<AddressedDNA> getListOligos(DNAContainer container, long listId) {
        BaseSequence seq = container.get(listId);
        if (seq == null)
            return Collections.emptyList();

        List<AddressedDNA> oligos = new ArrayList<>(Arrays.asList(container.getOligos(listId)));
        listId = DNAPacker.unpack(seq, false).longValue();
        seq = container.get(listId);
        while (seq != null) {
            oligos.addAll(Arrays.asList(container.getOligos(listId)));
            listId = DNAPacker.unpack(seq, false).longValue();
            seq = container.get(listId);
        }

        return oligos;
    }

    public static boolean appendToList(DNAContainer container, long id, BaseSequence seq) {
        return appendToList(container, id, Collections.singletonList(seq));
    }

    public static boolean appendToList(DNAContainer container, long id, List<BaseSequence> seqs) {
        if (seqs == null || seqs.isEmpty())
            return false;

        BaseSequence seq = container.get(id);
        if (seq == null)
            return false;

        do {
            id = DNAPacker.unpack(seq, false).longValue();
            seq = container.get(id);
        } while(seq != null);

        long[] ids = container.registerIds(seqs.size());
        container.put(id, BaseSequence.join(DNAPacker.pack(ids[0], DNAPacker.LengthBase.INT_64), seqs.get(0)));
        for (int i = 1; i < seqs.size(); i++)
            container.put(ids[i - 1], BaseSequence.join(DNAPacker.pack(ids[i], DNAPacker.LengthBase.INT_64), seqs.get(i)));

        return true;
    }


    public static boolean insertIntoList(DNAContainer container, long id, int pos, BaseSequence seq) {
        if (pos < 0)
            return false;

        var atm = container.getAddressManager();
        long idNew = container.registerId();
        long idNewRouted = atm.route(idNew).routed();

        Long idPrevious = pos == 0 ? Long.valueOf(id) : FuncUtils.stream(() -> getRichListIterator(container, id)).skip(pos - 1).findFirst().map(Pair::getT2).orElse(null);
        if (idPrevious == null)
            return appendToList(container, id, seq);

        long idNext = container.registerId();
        long idPreviousRouted = atm.route(idPrevious).routed();

        container.put(idNew, BaseSequence.join(DNAPacker.pack(idNext, DNAPacker.LengthBase.INT_64), seq));

        atm.addressRoutingManager().put(new RoutingManager.RoutedAddress<>(idPrevious, idNewRouted));
        atm.addressRoutingManager().put(new RoutingManager.RoutedAddress<>(idNext, idPreviousRouted));

        return true;
    }
}

