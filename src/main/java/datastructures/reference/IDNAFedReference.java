package datastructures.reference;

import core.BaseSequence;
import utils.AddressedDNA;
import java.util.Arrays;
import java.util.function.Function;

public interface IDNAFedReference<T, S extends IDNASketch> extends IDNAReference<S> {
    T decode();

    static <T, S extends IDNASketch, R extends IDNAReference<S>> IDNAFedReference<T, S> of(R reference, Function<R, T> decoder) {
        return new DNAFedReference<>(
                reference.sketch(),
                __ -> reference.payloads(),
                __ -> decoder.apply(reference)
        );
    }

    static <T, S extends IDNASketch> IDNAFedReference<T, S> of(S sketch, BaseSequence[] payloads, Function<BaseSequence[], T> payloadsDecoder) {
        return new DNAFedReference<>(sketch, __ -> payloads, r -> payloadsDecoder.apply(r.payloads()));
    }

    static <T, S extends IDNASketch> IDNAFedReference<T, S> of(S sketch, Function<S, BaseSequence[]> payloadExtractor, Function<BaseSequence[], T> payloadsDecoder) {
        return new DNAFedReference<>(sketch, payloadExtractor, r -> payloadsDecoder.apply(r.payloads()));
    }

    static <T> IDNAFedReference<T, IDNASketch.ContainerIdSketch> of(IDNASketch.ContainerIdSketch sketch, Function<BaseSequence[], T> payloadsDecoder) {
        return new DNAFedReference<>(
                sketch,
                s -> Arrays.stream(s.container().getOligos(s.id())).map(AddressedDNA::payload).toArray(BaseSequence[]::new),
                r -> payloadsDecoder.apply(r.payloads())
        );
    }

    class DNAFedReference<T, S extends IDNASketch> extends DNAReference<S> implements IDNAFedReference<T, S> {
        protected final Function<IDNAReference<S>, T> decoder;

        public DNAFedReference(S sketch, Function<S, BaseSequence[]> payloadExtractor, Function<IDNAReference<S>, T> decoder) {
            super(sketch, payloadExtractor);
            this.decoder = decoder;
        }

        @Override
        public T decode() {
            return decode(decoder);
        }

        public T decode(Function<IDNAReference<S>, T> decoder) {
            return decoder.apply(this);
        }
    }
}
