package datastructures.container;

import core.BaseSequence;
import datastructures.container.impl.SizedDNAContainer;
import utils.AddressedDNA;
import java.util.Arrays;
import java.util.Collection;

public interface DNAContainer extends Container<Long, BaseSequence, BaseSequence> {
    Container<Long, AddressedDNA[], ?> getOligoStore();
    int getAddressSize();
    int getPayloadSize();

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

    static SizedDNAContainer.Builder builder() {
        return SizedDNAContainer.builder();
    }
}
