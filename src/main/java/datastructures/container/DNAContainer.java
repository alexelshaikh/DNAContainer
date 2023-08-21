package datastructures.container;

import core.BaseSequence;
import datastructures.container.impl.SizedDNAContainer;
import datastructures.container.translation.AddressManager;
import utils.AddressedDNA;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public interface DNAContainer extends Container<Long, BaseSequence> {
    DNAStorage getOligoStore();
    BaseSequence assembleFromOligos(AddressedDNA[] oligos);

    AddressManager<Long, BaseSequence> getAddressManager();

    long put(BaseSequence seq);

    long registerId();
    long[] registerIds(int n);
    int getAddressSize();
    int getPayloadSize();

    default BaseSequence assembleFromOligos(BaseSequence[] oligos) {
        int addrSize = getAddressSize();
        return assembleFromOligos(Arrays.stream(oligos).map(o -> AddressedDNA.of(o, addrSize)).toArray(AddressedDNA[]::new));
    }
    @Override
    default Collection<BaseSequence> values() {
        return getSegmentedOligos().stream().flatMap(a -> Arrays.stream(a).map(AddressedDNA::join)).toList();
    }

    default BaseSequence[] getAddresses(Long id) {
        return Arrays.stream(getOligos(id)).map(AddressedDNA::address).toArray(BaseSequence[]::new);
    }

    default int getSegmentsCount(Long id) {
        return getAddresses(id).length;
    }

    default AddressedDNA[] getOligos(Long id) {
        return getOligoStore().get(id);
    }

    default Collection<AddressedDNA[]> getSegmentedOligos() {
        return getOligoStore().values();
    }

    @Override
    default boolean remove(Long key) {
        boolean removed = getOligoStore().remove(key);
        if (removed) {
            AddressManager<Long, BaseSequence> am = getAddressManager();
            am.remove(am.route(key));
        }
        return removed;
    }

    static SizedDNAContainer.Builder builder() {
        return SizedDNAContainer.builder();
    }

    abstract class DNAStorage implements Container<AddressManager.ManagedAddress<Long, BaseSequence>, AddressedDNA[]> {

        protected final AddressManager<Long, BaseSequence> am;

        public DNAStorage(AddressManager<Long, BaseSequence> am) {
            this.am = am;
        }


        public abstract void put(long key, AddressedDNA[] value);
        public abstract boolean remove(long key);
        public abstract AddressedDNA[] get(long key);


        @Override
        public Set<AddressManager.ManagedAddress<Long, BaseSequence>> keys() {
            return am.addressRoutingManager().stream().map(routed -> new AddressManager.LazyManagedAddress<>(routed.original(), routed.routed(), () -> am.translate(routed).translated())).collect(Collectors.toSet());
        }
    }
}
