package datastructures.container.impl;

import core.BaseSequence;
import core.dnarules.BasicDNARules;
import core.dnarules.DNARule;
import datastructures.container.Container;
import datastructures.container.DNAContainer;
import datastructures.container.translation.AddressManager;
import datastructures.container.translation.DNAAddrManager;
import dnacoders.BasicSegmentationCoder;
import dnacoders.PayloadDistanceCoder;
import dnacoders.SegmentationCoder;
import dnacoders.headercoders.BasicDNAPadder;
import dnacoders.headercoders.ReedSolomonCoder;
import utils.*;
import utils.lsh.LSH;
import utils.lsh.minhash.MinHashLSH;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class SizedDNAContainer extends Container.LinearLongContainer<BaseSequence> implements DNAContainer {

    private final SegmentationCoder segmentationCoder;
    private final LSH<BaseSequence> oligLSH;
    private final int payloadSize;
    private final DNAPacker.LengthBase sizedHeaderLengthBase;
    private final DNAStorage store;
    private final Coder<AddressedDNA, AddressedDNA> payloadDistanceCoder;
    private final AddressManager<Long, BaseSequence> addressManager;
    private final int payloadOffset;
    private final boolean isParallel;

    private SizedDNAContainer(
            boolean isParallel,
            SegmentationCoder segmentationCoder,
            Coder<AddressedDNA, AddressedDNA> payloadDistanceCoder,
            LSH<BaseSequence> oligLSH,
            int payloadSize,
            DNAPacker.LengthBase sizedHeaderLengthBase,
            DNAStorage store,
            AddressManager<Long, BaseSequence> addressManager) {

        super();
        this.segmentationCoder = segmentationCoder;
        this.oligLSH = oligLSH;
        this.payloadSize = payloadSize;
        this.sizedHeaderLengthBase = sizedHeaderLengthBase;
        this.store = store;
        this.addressManager = addressManager;
        this.payloadOffset = sizedHeaderLengthBase.totalSize() + DNAPacker.LengthBase.INT_64.totalSize();
        this.payloadDistanceCoder = payloadDistanceCoder;
        this.isParallel = isParallel;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void put(Long id, BaseSequence seq) {
        if (id >= gen.getCurrentNextFreeId())
            throw new RuntimeException("id=" + id + " not registered");

        int numSegmentsMinusOne = segmentationCoder.numSegments(seq.length() + sizedHeaderLengthBase.totalSize() + DNAPacker.LengthBase.INT_64.totalSize()) - 1;
        if (DNAPacker.pack(numSegmentsMinusOne, sizedHeaderLengthBase).length() > sizedHeaderLengthBase.totalSize())
            throw new RuntimeException("payload too large for the specified sizedHeaderLengthBase. Consider using a larger sizedHeaderLengthBase (Packer.Type)!");

        List<AddressManager.ManagedAddress<Long, BaseSequence>> managedIds = registerManagedIds(numSegmentsMinusOne);

        BaseSequence sizedPayload = BaseSequence.join(
                DNAPacker.pack(managedIds.isEmpty() ? -1L : managedIds.get(0).original(), DNAPacker.LengthBase.INT_64),
                DNAPacker.pack(numSegmentsMinusOne, sizedHeaderLengthBase),
                seq);

        var segments = segmentationCoder.encode(sizedPayload);
        var addressedSegments = new AddressedDNA[segments.length];
        var rootId = addressManager.routeAndTranslate(id);
        addressedSegments[0] = payloadDistanceCoder.encode(new AddressedDNA(rootId.translated(), segments[0]));
        store.put(rootId, addressedSegments[0]);

        FuncUtils.stream(IntStream.range(0, managedIds.size()), isParallel).forEach(i -> {
            addressedSegments[i + 1] = payloadDistanceCoder.encode(new AddressedDNA(managedIds.get(i).translated(), segments[i + 1]));
            store.put(managedIds.get(i), addressedSegments[i + 1]);
        });
    }

    @Override
    public AddressedDNA[] getOligos(long id) {
        AddressedDNA root = store.get(id);
        if (root == null)
            return null;

        var payload = payloadDistanceCoder.decode(root).payload();
        BaseSequence header = segmentationCoder.decode(new BaseSequence[] {payload});

        long nextId = DNAPacker.LengthBase.INT_64.unpackSingle(header).longValue();
        int numSegments = sizedHeaderLengthBase.unpackSingle(header.window(DNAPacker.LengthBase.INT_64.totalSize())).intValue() + 1;
        AddressedDNA[] oligos = new AddressedDNA[numSegments];
        oligos[0] = root;

        int c = 1;
        while(c < numSegments)
            oligos[c++] = store.get(nextId++);

        return oligos;
    }

    @Override
    public BaseSequence get(Long id) {
        var oligos = getOligos(id);
        return oligos == null? null : assembleFromOligos(oligos);
    }

    @Override
    public long size() {
        return store.size();
    }

    public BaseSequence getSized(long id) {
        var oligos = getOligos(id);
        return oligos == null? null : assembleSizedFromOligos(oligos);
    }

    @Override
    public DNAStorage getOligoStore() {
        return store;
    }

    public int unpackSizeFromSizedPayload(BaseSequence sizedPayload) {
        return DNAPacker.unpack(sizedPayload.window(DNAPacker.LengthBase.INT_64.totalSize()), false).intValue() + 1;
    }

    @Override
    public BaseSequence assembleFromOligos(AddressedDNA[] oligos) {
        return assembleSizedFromOligos(oligos).window(payloadOffset);
    }

    private BaseSequence assembleSizedFromOligos(AddressedDNA[] oligos) {
        return segmentationCoder.decode(Arrays.stream(oligos).map(payloadDistanceCoder::decode).map(AddressedDNA::payload).toArray(BaseSequence[]::new));
    }

    @Override
    public int getAddressSize() {
        return addressManager.addressSize();
    }

    @Override
    public int getPayloadSize() {
        return payloadSize;
    }

    public LSH<BaseSequence> getOligoLSH() {
        return oligLSH;
    }

    @Override
    public AddressManager<Long, BaseSequence> getAddressManager() {
        return addressManager;
    }

    public static class Builder {
        public static final int DEFAULT_PAYLOAD_SIZE = 150;
        public static final int DEFAULT_PAYLOAD_ECC_SIZE = 0;
        public static final double DEFAULT_TARGET_GC_CONTENT = 0.5;

        public static final int DEFAULT_PARTITIONED_PAYLOAD_SIZE = 300;
        public static final Function<Integer, Boolean> DEFAULT_PARTITIONED_PAYLOAD_DISTANCE_CODER = size -> size > DEFAULT_PARTITIONED_PAYLOAD_SIZE;

        public static final boolean DEFAULT_IS_PARALLEL = false;

        public static final float DEFAULT_OLIGO_ERROR_WEIGHT = 1.0f;
        public static final float DEFAULT_OLIGO_DISTANCE_WEIGHT = 1.0f;
        public static final BiFunction<Integer, Integer, LSH<BaseSequence>> DEFAULT_OLIGO_LSH = (addrSize, payloadSize) -> MinHashLSH.newSeqLSHTraditional(1 + 4 * Math.max(1, (addrSize + payloadSize) / 200), 5);

        public static final int DEFAULT_PAYLOAD_NUM_PERMUTATIONS = 16;
        public static final DNAPacker.LengthBase DEFAULT_SIZED_HEADER = DNAPacker.LengthBase.SHORT;
        public static final DNARule DEFAULT_DNA_RULES = BasicDNARules.INSTANCE;

        public static final Function<AddressManager<Long, BaseSequence>, DNAStorage> DEFAULT_STORE_NOT_PERSISTENT = DNAStorageMap::new;
        public static final BiFunction<AddressManager<Long, BaseSequence>, Integer, DNAStorage> DEFAULT_STORE_PERSISTENT = (am, payloadSize) -> new DNAStorageDisk(am, payloadSize, "dnacontainer.store");

        public static final Supplier<DNAAddrManager> DEFAULT_ADDRESS_TRANSLATION_MANAGER_SUPP =
                () -> DNAAddrManager
                        .builder()
                        .build();

        private DNARule dnaRules;

        private Integer payloadSize;
        private Integer payloadEccSize;
        private Integer numGcCorrectionsPayload;

        private Boolean partitionedPayloadDistanceCoder;
        private Integer partitionedPayloadSize;

        private Double targetGcContent;

        private Float oligoErrorWeight;
        private Float oligoDistanceWeight;

        private Integer numPayloadPermutations;

        private Boolean isParallel;

        private DNAStoreType storeType;

        private DNAPacker.LengthBase sizedHeaderLengthBase;
        private DNAStorage store;
        private AddressManager<Long, BaseSequence> addressManager;
        private LSH<BaseSequence> oligoLSH;

        public <T> RichDNAContainer<T> buildToRichContainer(Coder<T, BaseSequence> coder) {
            return build().toRichContainer(coder);
        }

        public RichDNAContainer<BaseSequence> buildToRichContainer() {
            return build().toRichContainer(Coder.identity());
        }

        public Builder() {
            this.storeType = DNAStoreType.MEMORY_MAP;
        }

        public SizedDNAContainer build() {
            this.addressManager = FuncUtils.nullEscape(addressManager, DEFAULT_ADDRESS_TRANSLATION_MANAGER_SUPP::get);
            this.payloadSize = FuncUtils.conditionOrElse(x -> x != null && x > 0, payloadSize, () -> DEFAULT_PAYLOAD_SIZE);
            this.payloadEccSize = FuncUtils.conditionOrElse(x -> x != null && x >= 0, payloadEccSize, () -> DEFAULT_PAYLOAD_ECC_SIZE);
            if (payloadEccSize > 0 && payloadSize % 4 != 0)
                throw new RuntimeException("L_payload% 4 != 0");

            this.oligoErrorWeight = FuncUtils.nullEscape(oligoErrorWeight, () -> DEFAULT_OLIGO_ERROR_WEIGHT);
            this.oligoDistanceWeight = FuncUtils.nullEscape(oligoDistanceWeight, () -> DEFAULT_OLIGO_DISTANCE_WEIGHT);
            this.sizedHeaderLengthBase = FuncUtils.nullEscape(sizedHeaderLengthBase, () -> DEFAULT_SIZED_HEADER);

            this.isParallel = FuncUtils.nullEscape(isParallel, DEFAULT_IS_PARALLEL);

            this.numPayloadPermutations = FuncUtils.conditionOrElse(x -> x != null && x >= 0, numPayloadPermutations, () -> DEFAULT_PAYLOAD_NUM_PERMUTATIONS);
            this.dnaRules = FuncUtils.nullEscape(dnaRules, DEFAULT_DNA_RULES);
            this.targetGcContent = FuncUtils.nullEscape(targetGcContent, DEFAULT_TARGET_GC_CONTENT);
            ReedSolomonCoder payloadEccCoder = FuncUtils.conditionOrElse(x -> payloadEccSize <= 0, null, () -> new ReedSolomonCoder(payloadEccSize));

            int payloadEccOverhead = FuncUtils.conditionOrElse(x -> payloadEccCoder == null, 0, () -> payloadEccCoder.overhead(payloadSize));
            this.oligoLSH = FuncUtils.conditionOrElse(Objects::nonNull, oligoLSH, () -> DEFAULT_OLIGO_LSH.apply(addressManager.addressSize(), payloadSize));

            this.partitionedPayloadDistanceCoder = FuncUtils.nullEscape(partitionedPayloadDistanceCoder, DEFAULT_PARTITIONED_PAYLOAD_DISTANCE_CODER.apply(payloadSize));
            this.partitionedPayloadSize = FuncUtils.nullEscape(partitionedPayloadSize, DEFAULT_PARTITIONED_PAYLOAD_SIZE);

            PayloadDistanceCoder payloadDistanceCoder = new PayloadDistanceCoder(
                    false,
                    partitionedPayloadDistanceCoder,
                    partitionedPayloadSize,
                    oligoLSH,
                    dnaRules,
                    payloadEccCoder != null ? payloadEccCoder :  Coder.identity(),
                    numPayloadPermutations,
                    oligoErrorWeight,
                    oligoDistanceWeight
            );

            boolean numGcCorrectionsPayloadIsSet = numGcCorrectionsPayload != null;
            this.numGcCorrectionsPayload = FuncUtils.conditionOrElse(x -> numGcCorrectionsPayloadIsSet && x >= 0, numGcCorrectionsPayload, () -> (int) (0.1f * payloadSize));

            int targetLen = payloadSize - payloadEccOverhead - payloadDistanceCoder.permutationOverhead();
            int headerSize = sizedHeaderLengthBase.totalSize() + DNAPacker.LengthBase.INT_64.totalSize();
            int minPayloadSizeNoGcs = 3 + headerSize + payloadEccOverhead + payloadDistanceCoder.permutationOverhead();
            int minPayloadSize = minPayloadSizeNoGcs + FuncUtils.conditionOrElse(x -> numGcCorrectionsPayloadIsSet, numGcCorrectionsPayload, () -> (int) (0.1f * minPayloadSizeNoGcs));
            int splitLen = targetLen - 3 - numGcCorrectionsPayload;

            if (splitLen <= 0 || splitLen < headerSize)
                throw new RuntimeException("payloadSize (" + payloadSize + ") is too small. Please increase the payload's size to at least " + minPayloadSize);

            BasicSegmentationCoder segmentationCoder = new BasicSegmentationCoder(targetLen, numGcCorrectionsPayload, BasicDNAPadder.FILLER_RANDOM_CUSTOM_GC.apply(targetGcContent));

            this.store = switch (storeType) {
                case MEMORY_MAP -> DEFAULT_STORE_NOT_PERSISTENT.apply(addressManager);
                case DISK_PERSISTENT -> DEFAULT_STORE_PERSISTENT.apply(addressManager, payloadSize);
                case CUSTOM -> store;
            };

            return new SizedDNAContainer(
                    isParallel,
                    segmentationCoder,
                    payloadDistanceCoder,
                    oligoLSH,
                    payloadSize,
                    sizedHeaderLengthBase,
                    store,
                    addressManager
            );
        }

        public Builder setPayloadSize(int payloadSize) {
            this.payloadSize = payloadSize;
            return this;
        }

        public Builder setPartitionedPayloadDistanceCoder(boolean partitionedPayloadDistanceCoder) {
            this.partitionedPayloadDistanceCoder = partitionedPayloadDistanceCoder;
            return this;
        }

        public Builder setPartitionedPayloadSize(int partitionedPayloadSize) {
            this.partitionedPayloadSize = partitionedPayloadSize;
            return this;
        }

        public Builder setSizedHeaderLengthBase(DNAPacker.LengthBase sizedHeaderLengthBase) {
            this.sizedHeaderLengthBase = sizedHeaderLengthBase;
            return this;
        }

        public Builder setStore(DNAStorage store) {
            this.store = store;
            if (store == null)
                throw new RuntimeException("store == null");
            this.storeType = DNAStoreType.CUSTOM;
            return this;
        }

        public Builder setOligoLSH(LSH<BaseSequence> oligoLSH) {
            this.oligoLSH = oligoLSH;
            return this;
        }

        public Builder setAddressManager(AddressManager<Long, BaseSequence> addressManager) {
            if (!addressManager.addressIsFixedSize())
                throw new RuntimeException("the provided AddressManager does not support fixed address size L_address");

            this.addressManager = addressManager;
            return this;
        }

        public Builder setPayloadEccSize(int payloadEccSize) {
            this.payloadEccSize = payloadEccSize;
            return this;
        }

        public Builder setParallel(boolean parallel) {
            this.isParallel = parallel;
            return this;
        }

        public Builder setNumGcCorrectionsPayload(int numGcCorrectionsPayload) {
            this.numGcCorrectionsPayload = numGcCorrectionsPayload;
            return this;
        }

        public Builder setDnaRules(DNARule dnaRules) {
            this.dnaRules = dnaRules;
            return this;
        }

        public Builder setOligoErrorWeight(float oligoErrorWeight) {
            this.oligoErrorWeight = oligoErrorWeight;
            return this;
        }

        public Builder setOligoDistanceWeight(float oligoDistanceWeight) {
            this.oligoDistanceWeight = oligoDistanceWeight;
            return this;
        }

        public Builder setNumPayloadPermutations(int numPayloadPermutations) {
            this.numPayloadPermutations = numPayloadPermutations;
            return this;
        }

        public Builder setStorePersistentDefault() {
            this.storeType = DNAStoreType.DISK_PERSISTENT;
            return this;
        }
        public Builder setStoreNotPersistentDefault() {
            this.storeType = DNAStoreType.MEMORY_MAP;
            return this;
        }

        private enum DNAStoreType {
            MEMORY_MAP, CUSTOM, DISK_PERSISTENT
        }
    }
}
