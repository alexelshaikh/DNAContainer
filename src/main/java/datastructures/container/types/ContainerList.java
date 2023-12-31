package datastructures.container.types;

import core.BaseSequence;
import datastructures.container.DNAContainer;
import datastructures.reference.IDNAFedReference;
import datastructures.reference.IDNASketch;
import utils.*;
import java.util.*;
import java.util.stream.Stream;

public class ContainerList<T> extends IDNAFedReference.DNAFedReference<List<T>, IDNASketch.ContainerIdSketch> implements Streamable<T> {

    private final Coder<T, BaseSequence> coder;

    public ContainerList(DNAContainer container, IDNASketch.ContainerIdSketch sketch, Coder<T, BaseSequence> coder) {
        super(
                sketch,
                s -> getListOligos(container, s.id()).stream().map(AddressedDNA::payload).toArray(BaseSequence[]::new),
                s -> FuncUtils.stream(() -> getRichListIterator(container, sketch.id())).map(RichListElement::object).map(coder::decode).toList()
        );
        this.coder = coder;
    }

    @Override
    public Iterator<T> iterator() {
        return stream().iterator();
    }

    @Override
    public Stream<T> stream() {
        return encodedObjectsStream().map(coder::decode);
    }

    public Stream<BaseSequence> encodedObjectsStream() {
        return FuncUtils.stream(this::getRichListIterator).map(RichListElement::object);
    }

    @Override
    public List<T> decode() {
        return stream().toList();
    }

    public T get(int i) {
        return stream().skip(i).findFirst().orElse(null);
    }

    @Override
    public int size() {
        return (int) encodedObjectsStream().count();
    }

    @Override
    public BaseSequence[] addresses() {
        return getListOligos(sketch.container(), sketch.id()).stream().map(AddressedDNA::address).toArray(BaseSequence[]::new);
    }

    private Iterator<RichListElement> getRichListIterator() {
        return getRichListIterator(sketch);
    }

    public static Iterator<RichListElement> getRichListIterator(IDNASketch.ContainerIdSketch sketch) {
        return getRichListIterator(sketch.container(), sketch.id());
    }

    public static Iterator<RichListElement> getRichListIterator(DNAContainer container, long listId) {
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
            public RichListElement next() {
                if (!hasNext())
                    throw new NoSuchElementException("Iterator exhausted! No more elements to return.");
                BaseSequence r = s.window(DNAPacker.LengthBase.INT_64.totalSize());
                id = DNAPacker.unpack(s, false).longValue();
                s = container.get(id);
                return new RichListElement(id, r);
            }
        };
    }

    public static List<AddressedDNA> getListOligos(IDNASketch.ContainerIdSketch sketch) {
        return getListOligos(sketch.container(), sketch.id());
    }

    public static List<AddressedDNA> getListOligos(DNAContainer container, long listId) {
        BaseSequence seq = container.get(listId);
        if (seq == null)
            return Collections.emptyList();

        List<AddressedDNA> oligos = new ArrayList<>(Arrays.asList(container.getOligos(listId)));
        long id = DNAPacker.unpack(seq, false).longValue();
        seq = container.get(id);
        while (seq != null) {
            oligos.addAll(Arrays.asList(container.getOligos(id)));
            id = DNAPacker.unpack(seq, false).longValue();
            seq = container.get(id);
        }

        return oligos;
    }


    public boolean append(T element) {
        return append(Collections.singletonList(element));
    }

    public boolean append(List<T> elements) {
        if (elements == null || elements.isEmpty())
            return false;

        long id = sketch.id();
        DNAContainer container = sketch.container();
        BaseSequence seq = container.get(id);
        if (seq == null)
            return false;

        do {
            id = DNAPacker.unpack(seq, false).longValue();
            seq = container.get(id);
        } while(seq != null);

        long[] ids = container.registerIds(elements.size());
        container.put(id, BaseSequence.join(DNAPacker.pack(ids[0], DNAPacker.LengthBase.INT_64), coder.encode(elements.get(0))));
        for (int i = 1; i < elements.size(); i++)
            container.put(ids[i - 1], BaseSequence.join(DNAPacker.pack(ids[i], DNAPacker.LengthBase.INT_64), coder.encode(elements.get(i))));

        return true;
    }

    public boolean insert(int pos, T element) {
        if (pos < 0)
            return false;

        DNAContainer container = sketch.container();
        var atm = container.getAddressManager();
        long idNew = container.registerId();
        long idNewRouted = atm.routeAndTranslate(idNew).routed();
        long id = sketch.id();

        Long idPrevious = pos == 0 ? Long.valueOf(id) : FuncUtils.stream(() -> getRichListIterator(container, id)).skip(pos - 1).findFirst().map(RichListElement::id).orElse(null);
        if (idPrevious == null)
            return append(element);

        long idNext = container.registerId();
        long idPreviousRouted = atm.routeAndTranslate(idPrevious).routed();

        container.put(idNew, BaseSequence.join(DNAPacker.pack(idNext, DNAPacker.LengthBase.INT_64), coder.encode(element)));

        atm.addressRoutingManager().put(idPrevious, idNewRouted);
        atm.addressRoutingManager().put(idNext, idPreviousRouted);

        return true;
    }

    public static <T> ContainerList<T> putList(DNAContainer container, Coder<T, BaseSequence> coder, List<T> seqs) {
        return putList(container, coder, container.registerId(), seqs);
    }

    public static <T> ContainerList<T> putList(DNAContainer container, Coder<T, BaseSequence> coder, long rootId, List<T> list) {
        if (list == null || list.isEmpty())
            return putEmptyList(container, coder, rootId);

        int listSize = list.size();
        long[] ids = container.registerIds(listSize);

        container.put(rootId, BaseSequence.join(DNAPacker.pack(ids[0], DNAPacker.LengthBase.INT_64), coder.encode(list.get(0))));
        for (int i = 1; i < listSize; i++)
            container.put(ids[i - 1], BaseSequence.join(DNAPacker.pack(ids[i], DNAPacker.LengthBase.INT_64), coder.encode(list.get(i))));

        return new ContainerList<>(container, new IDNASketch.ContainerIdSketch(rootId, container), coder);
    }

    public static <T> ContainerList<T> putEmptyList(DNAContainer container, Coder<T, BaseSequence> coder, long rootId) {
        container.put(rootId, DNAPacker.pack(container.registerId(), DNAPacker.LengthBase.INT_64));
        return new ContainerList<>(container, new IDNASketch.ContainerIdSketch(rootId, container), coder);
    }

    @Override
    public String toString() {
        return getListOligos(sketch().container(), sketch.id()).toString();
    }

    public record RichListElement(long id, BaseSequence object) {

    }
}
