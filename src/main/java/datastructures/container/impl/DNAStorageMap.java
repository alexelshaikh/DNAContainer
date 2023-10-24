package datastructures.container.impl;

import core.BaseSequence;
import datastructures.container.DNAContainer;
import datastructures.container.translation.AddressManager;
import utils.AddressedDNA;
import java.util.Collection;

public class DNAStorageMap extends DNAContainer.DNAStorage {

    final MapContainer<Long, AddressedDNA> map;

    public DNAStorageMap(AddressManager<Long, BaseSequence> am) {
        super(am);
        this.map = new MapContainer<>();
    }

    @Override
    public void put(AddressManager.ManagedAddress<Long, BaseSequence> key, AddressedDNA value) {
        map.put(key.routed(), value);
    }

    public MapContainer<Long, AddressedDNA> getMap() {
        return map;
    }

    @Override
    public boolean remove(AddressManager.ManagedAddress<Long, BaseSequence> key) {
        return map.remove(key.routed());
    }

    @Override
    public AddressedDNA get(AddressManager.ManagedAddress<Long, BaseSequence> key) {
        return map.get(key.routed());
    }

    @Override
    public long size() {
        return map.size();
    }

    @Override
    public Collection<AddressedDNA> values() {
        return map.values();
    }

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public void put(long key, AddressedDNA value) {
        map.put(am.route(key).routed(), value);
    }

    @Override
    public boolean remove(long key) {
        var routed = am.addressRoutingManager().get(key).routed();
        return routed != null && map.remove(routed);
    }

    @Override
    public AddressedDNA get(long key) {
        var routed = am.addressRoutingManager().get(key).routed();
        return routed == null ? null : map.get(routed);
    }
}
