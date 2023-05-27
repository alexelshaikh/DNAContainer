package utils.rq;

import core.Base;
import core.BaseSequence;
import core.dnarules.BasicDNARules;
import net.fec.openrq.ArrayDataDecoder;
import net.fec.openrq.EncodingPacket;
import net.fec.openrq.OpenRQ;
import net.fec.openrq.decoder.SourceBlockDecoder;
import net.fec.openrq.encoder.SourceBlockEncoder;
import net.fec.openrq.parameters.FECParameters;
import net.fec.openrq.parameters.ParameterChecker;
import utils.Coder;
import utils.DNAPacker;
import utils.FuncUtils;
import utils.Pair;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class RQCoder implements Coder<byte[], BaseSequence> {

    public static final Function<Integer, Integer> DEFAULT_SYMBOL_SIZE_FUNC = len -> Math.max(1, len / 8);

    public static final RQCoder DEFAULT_RQ = new RQCoder(packet -> BasicDNARules.INSTANCE.evalErrorProbability(packet) <= 0.5f, strand -> BasicDNARules.INSTANCE.evalErrorProbability(strand) <= 0.5f, false);
    public static final RQCoder DEFAULT_RQ_PARALLEL = new RQCoder(packet -> BasicDNARules.INSTANCE.evalErrorProbability(packet) <= 0.5f, strand -> BasicDNARules.INSTANCE.evalErrorProbability(strand) <= 0.5f, true);

    public static final int NUM_SOURCE_BLOCKS = 1;
    public static final int TWO_INTS_BYTES = 2 * Integer.BYTES;
    public static final Base CUSTOM_SYMBOL_BASE = Base.T;
    public static final Base COPY_SYMBOL_BASE = Base.A;
    public static final BaseSequence CUSTOM_SYMBOL_SEQ = new BaseSequence(CUSTOM_SYMBOL_BASE).window(0);
    public static final BaseSequence COPY_SYMBOL_SEQ = new BaseSequence(COPY_SYMBOL_BASE).window(0);

    private final Function<BaseSequence, Boolean> packetRule;
    private final Function<BaseSequence, Boolean> strandRule;
    private final Function<Integer, Integer> symbolSizeFunc;

    private final int initialEsi;
    private final int packetsOverhead;
    private final boolean parallel;


    /**
     * Creates an RQ instance.
     * @param packetRule the rule that every packet has to fulfil.
     * @param strandRule the rule that the final DNA strand has to fulfil.
     * @param packetsOverhead the number of overhead packets ε
     * @param symbolSizeFunc the symbol size function.
     * @param initialEsi the starting encoding symbol id (ESI)
     */
    public RQCoder(Function<BaseSequence, Boolean> packetRule, Function<BaseSequence, Boolean> strandRule, int packetsOverhead, int initialEsi, Function<Integer, Integer> symbolSizeFunc, boolean parallel) {
        this.packetRule = packetRule;
        this.strandRule = strandRule;
        this.initialEsi = initialEsi;
        this.packetsOverhead = packetsOverhead;
        this.symbolSizeFunc = symbolSizeFunc;
        this.parallel = parallel;
    }

    /**
     * Creates an RQ instance.
     * @param packetRule the rule that every packet has to fulfil.
     * @param strandRule the rule that the final DNA strand has to fulfil.
     * @param packetsOverhead the number of overhead packets ε
     * @param initialEsi the starting encoding symbol id (ESI)
     */
    public RQCoder(Function<BaseSequence, Boolean> packetRule, Function<BaseSequence, Boolean> strandRule, int packetsOverhead, int initialEsi) {
        this(packetRule, strandRule, packetsOverhead, initialEsi, DEFAULT_SYMBOL_SIZE_FUNC, false);
    }

    /**
     * Creates an RQ instance.
     * @param packetRule the rule that every packet has to fulfil.
     * @param strandRule the rule that the final DNA strand has to fulfil.
     */
    public RQCoder(Function<BaseSequence, Boolean> packetRule, Function<BaseSequence, Boolean> strandRule) {
        this(packetRule, strandRule, 0, 0, DEFAULT_SYMBOL_SIZE_FUNC, false);
    }

    /**
     * Creates an RQ instance.
     * @param packetRule the rule that every packet has to fulfil.
     * @param strandRule the rule that the final DNA strand has to fulfil.
     * @param parallel if true, packets are generated in parallel, and otherwise packets are generated sequentially.
     */
    public RQCoder(Function<BaseSequence, Boolean> packetRule, Function<BaseSequence, Boolean> strandRule, boolean parallel) {
        this(packetRule, strandRule, 0, 0, DEFAULT_SYMBOL_SIZE_FUNC, parallel);
    }

    /**
     * Creates an RQ instance.
     * @param packetRule the rule that every packet has to fulfil.
     * @param strandRule the rule that the final DNA strand has to fulfil.
     * @param packetsOverhead the number of overhead packets ε
     */
    public RQCoder(Function<BaseSequence, Boolean> packetRule, Function<BaseSequence, Boolean> strandRule, int packetsOverhead) {
        this(packetRule, strandRule, packetsOverhead, 0, DEFAULT_SYMBOL_SIZE_FUNC, false);
    }

    /**
     * Creates an RQ instance.
     * @param packetRule the rule that every packet has to fulfil.
     * @param strandRule the rule that the final DNA strand has to fulfil.
     * @param symbolSizeFunction the symbol size function.
     */
    public RQCoder(Function<BaseSequence, Boolean> packetRule, Function<BaseSequence, Boolean> strandRule, Function<Integer, Integer> symbolSizeFunction) {
        this(packetRule, strandRule, 0, 0, symbolSizeFunction, false);
    }

    @Override
    public BaseSequence encode(byte[] bytes) {
        return encode(bytes, initialEsi, packetsOverhead, symbolSizeFunc.apply(bytes.length));
    }

    /**
     * @param bytes Source data
     * @param initialEsi Defines the encoding symbol identifier (ESI) of the symbol in the first encoding packet.
     * @param packetOverhead Amount of additional packets to try if decode not successful after K packets
     * @return Source data encoded in a BaseSequence
     */
    public BaseSequence encode(byte[] bytes, int initialEsi, int packetOverhead, int symbolSize) {
        if (bytes.length < symbolSize)
            throw new RuntimeException("bytes.length must be at least " + symbolSize + " or larger");

        var params = FECParameters.newParameters(bytes.length, symbolSize, NUM_SOURCE_BLOCKS);
        var encoder = OpenRQ.newEncoder(bytes, params);
        var sourceBlockEncoder = encoder.sourceBlock(0);

        return encodeBlock(sourceBlockEncoder, () -> OpenRQ.newDecoder(params, packetOverhead).sourceBlock(0), initialEsi, sourceBlockEncoder.numberOfSourceSymbols(), bytes.length, symbolSize, parallel);
    }

    /**
     *
     * @param startEsi Defines the encoding symbol identifier (ESI) of the symbol in the first encoding packet.
     * @param packetsCount Packets per block.
     * @param encoder The source block encoder.
     * @param freshDecoder A decoder's supplier.
     * @param symbolSize the default symbol size.
     * @param numBytes Total number bytes to encode per block.
     * @return Single encoded block.
     */
    private BaseSequence encodeBlock(SourceBlockEncoder encoder, Supplier<SourceBlockDecoder> freshDecoder, int startEsi, int packetsCount, int numBytes, int symbolSize, boolean parallel) {
        int[] order;
        int currentPerm;
        int permutations;
        SourceBlockDecoder decoder = freshDecoder.get();
        List<Pair<EncodingPacket, BaseSequence>> goodPackets = new ArrayList<>();
        Iterator<EncodingPacket> it = encoder.newIterableBuilder().startAt(startEsi).endAt(ParameterChecker.maxEncodingSymbolID()).build().iterator();
        do {
            goodPackets.addAll(parallel? packetsParallel(it, packetsCount, symbolSize) : packets(it, packetsCount, symbolSize));
            order = IntStream.range(0, goodPackets.size()).toArray();
            permutations = goodPackets.size();
            currentPerm = 0;
            while (currentPerm < permutations) {
                var result = combinePackets(decoder, goodPackets, order, numBytes, symbolSize);
                if (result.status == StrandAssemblyResult.Status.FOUND) {
                    return result.finalizedSeq;
                }
                if (result.status == StrandAssemblyResult.Status.NOT_DECODABLE) {
                    packetsCount = result.missing;
                    decoder = freshDecoder.get();
                    break;
                }
                if (result.status == StrandAssemblyResult.Status.RULES_NOT_SATISFIED) {
                    FuncUtils.shuffle(order);
                    currentPerm++;
                    decoder = freshDecoder.get();
                }
            }
        } while(!decoder.isSourceBlockDecoded());

        throw new RuntimeException("failed encoding block");
    }

    /**
     * Generates the outer header and assembles the final DNA sequence
     */
    private BaseSequence finalizeStrand(RichEncodedStrand encodedStrand) {
        BaseSequence header = DNAPacker.pack(encodedStrand.numBytes, encodedStrand.symbolSize);
        return new BaseSequence(header, encodedStrand.combinedPackets);
    }

    /**
     * Decode the outer header to generate its RichEncodedStrand
     * @param finalizedStrand A DNA sequence containing an outer header, assembled with finalizeStrand()
     */
    private RichEncodedStrand deFinalizeStrand(BaseSequence finalizedStrand) {
        Number[] outerHeader = DNAPacker.unpack(finalizedStrand, 2, false);
        int numBytes = outerHeader[0].intValue();
        int symbolSize = outerHeader[1].intValue();

        return new RichEncodedStrand(
                numBytes,
                symbolSize,
                finalizedStrand.window(DNAPacker.getPackedSize(finalizedStrand, 2))
        );
    }

    private StrandAssemblyResult combinePackets(SourceBlockDecoder decoder, List<Pair<EncodingPacket, BaseSequence>> packets, int[] order, int numBytes, int symbolSize) {
        BaseSequence strand = new BaseSequence();
        Pair<EncodingPacket, BaseSequence> p;
        for (int index : order) {
            p = packets.get(index);
            decoder.putEncodingPacket(p.getT1());
            strand.append(p.getT2());
            if (decoder.isSourceBlockDecoded()) {
                RichEncodedStrand encodedStrand = new RichEncodedStrand(numBytes, symbolSize, strand);
                BaseSequence finalizedStrand = finalizeStrand(encodedStrand);
                if (!strandRule.apply(finalizedStrand))
                    return StrandAssemblyResult.rulesNotSatisfied(finalizedStrand);

                return StrandAssemblyResult.found(finalizedStrand);
            }
        }
        return StrandAssemblyResult.notDecodable(decoder.missingSourceSymbols().size());
    }

    @Override
    public byte[] decode(BaseSequence seq) {
        RichEncodedStrand encodedStrand = deFinalizeStrand(seq);
        BaseSequence packetBases = encodedStrand.combinedPackets;
        var decoder = OpenRQ.newDecoder(FECParameters.newParameters(encodedStrand.numBytes, encodedStrand.symbolSize, NUM_SOURCE_BLOCKS), packetsOverhead);
        int index = 0;

        SourceBlockDecoder blockDecoder = decoder.sourceBlock(0);
        while(index <= packetBases.length()) {
            // If the symbol size is 2 but the length of the byte array % 2 == 1,
            // the last packet will only contain one byte (rather than two) in the payload
            EncodingPacket decodedPacket = decodePacket(packetBases.window(index), encodedStrand.symbolSize, decoder);
            int headerLength = encodedStrand.symbolSize != decodedPacket.symbolsLength() ? 2 : 1;

            index += 1 + DNAPacker.getPackedSize(
                    packetBases.window(index + 1),
                    headerLength + decodedPacket.symbolsLength()
            );

            blockDecoder.putEncodingPacket(decodedPacket);
            if (decoder.isDataDecoded())
                return decoder.dataArray();

        }

        throw new RuntimeException("Failed decoding sequence after exhausting all packets");
    }

    /**
     * Encodes an RQ EncodingPacket as a DNA sequence.
     * @return the EncodingPacket's DNA sequence representation
     */
    private BaseSequence encodePacket(EncodingPacket packet, int symbolSize) {
        BaseSequence packetSeq = new BaseSequence();
        DNAPacker.packUnsigned(packetSeq, packet.fecPayloadID());
        var customSymbolLen = false;

        if(packet.symbolsLength() != symbolSize) {
            DNAPacker.packUnsigned(packetSeq, packet.symbolsLength());
            customSymbolLen = true;
        }

        ByteBuffer packetSymbols = packet.symbols();
        for (int i = 0; i < packet.symbolsLength(); i++)
            DNAPacker.packUnsigned(packetSeq, packetSymbols.get(i));

        return new BaseSequence(customSymbolLen ? CUSTOM_SYMBOL_SEQ : COPY_SYMBOL_SEQ, packetSeq);
    }

    private EncodingPacket decodePacket(BaseSequence packetBases, int originalSymbolLen, ArrayDataDecoder decoder) {
        var customSymbolLen = packetBases.get(0) == CUSTOM_SYMBOL_BASE;
        int headerLength = customSymbolLen ? 2 : 1;
        var header = DNAPacker.unpack(packetBases.window(1), headerLength, false);

        int fecId = header[0].intValue();
        int symbolsLength = customSymbolLen ? header[1].intValue() : originalSymbolLen;

        var unpacked = DNAPacker.unpack(packetBases.window(1), headerLength + symbolsLength, false);
        var payload = new byte[symbolsLength];
        for (int i = 0; i < symbolsLength; i++)
            payload[i] = unpacked[i + headerLength].byteValue();

        var payloadBuffer = ByteBuffer.allocate(TWO_INTS_BYTES + symbolsLength);
        payloadBuffer.putInt(fecId);
        payloadBuffer.putInt(symbolsLength);
        payloadBuffer.put(payload);
        return decoder.parsePacket(payloadBuffer.array(), false).value();
    }

    /**
     * @return a list of sufficient packets whose DNA sequence representations satisfy the DNARule.
     * Note: If the rules are strict, many packets will be generated and the FEC ID will get large. This increases the amount of bases per packet.
     */
    private List<Pair<EncodingPacket, BaseSequence>> packets(Iterator<EncodingPacket> it, int count, int symbolSize) {
        List<Pair<EncodingPacket, BaseSequence>> list = new ArrayList<>(count);
        EncodingPacket p;
        BaseSequence seq;
        do {
            p = it.next();
            seq = encodePacket(p, symbolSize);
            if (packetRule.apply(seq))
                list.add(new Pair<>(p, seq));

        } while(it.hasNext() && list.size() < count);

        return list;
    }

    /**
     * @return a list of sufficient packets whose DNA sequence representations satisfy the DNARule. This list is generated in parallel.
     * Note: If the rules are strict, many packets will be generated and the FEC ID will get large. This increases the amount of bases per packet.
     */
    private List<Pair<EncodingPacket, BaseSequence>> packetsParallel(Iterator<EncodingPacket> it, int count, int symbolSize) {
        return FuncUtils.stream(() -> it)
                .parallel()
                .map(p -> new Pair<>(p, encodePacket(p, symbolSize)))
                .filter(p -> packetRule.apply(p.getT2()))
                .limit(count).toList();
    }


    private record StrandAssemblyResult(Status status, BaseSequence finalizedSeq, int missing) {

        enum Status {
            FOUND,
            NOT_DECODABLE,
            RULES_NOT_SATISFIED
        }

        static StrandAssemblyResult found(BaseSequence seq) {
            return new StrandAssemblyResult(Status.FOUND, seq, 0);
        }

        static StrandAssemblyResult notDecodable(int missing) {
            return new StrandAssemblyResult(Status.NOT_DECODABLE, null, missing);
        }

        static StrandAssemblyResult rulesNotSatisfied(BaseSequence seq) {
            return new StrandAssemblyResult(Status.RULES_NOT_SATISFIED, seq, 0);
        }

    }

    record RichEncodedStrand(int numBytes, int symbolSize, BaseSequence combinedPackets) {

    }
}