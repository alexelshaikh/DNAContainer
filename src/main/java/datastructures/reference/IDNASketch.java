package datastructures.reference;

import core.BaseSequence;
import datastructures.container.DNAContainer;

public interface IDNASketch {

    BaseSequence[] addresses();

    record ContainerIdSketch(long id, DNAContainer container) implements IDNASketch {

        @Override
        public BaseSequence[] addresses() {
            return container.getAddresses(id);
        }
    }
}
