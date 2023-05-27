package packaging;

import core.BaseSequence;
import core.dnarules.BasicDNARules;
import core.dnarules.DNARulesCollection;
import datastructures.container.AddressTranslationManager;
import datastructures.container.Container;
import datastructures.container.impl.PersistentContainer;
import datastructures.container.impl.SizedDNAContainer;
import datastructures.container.translation.DNAAddrTranslationManager;
import dnacoders.dnaconvertors.Bin;
import dnacoders.dnaconvertors.RotatingQuattro;
import dnacoders.dnaconvertors.RotatingTre;
import org.json.JSONObject;
import utils.*;
import utils.csv.BufferedCsvReader;
import utils.csv.CsvLine;
import utils.lsh.LSH;
import utils.lsh.minhash.BitMinHashLSH;
import utils.lsh.minhash.MinHashLSH;
import utils.lsh.minhash.MinHashLSHLight;
import utils.rq.RQCoder;
import utils.serializers.FixedSizeSerializer;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class Application {

    public static final Coder<String, byte[]> BIJECTIVE_STRING_CODER = new Coder<>() {
        @Override
        public  byte[] encode(String input) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(Short.BYTES * input.length());
            for (int i = 0; i < input.length(); i++)
                byteBuffer.putShort((short) input.charAt(i));

            return byteBuffer.array();
        }

        @Override
        public String decode(byte[] input) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(input);
            StringBuilder result = new StringBuilder();
            while (byteBuffer.hasRemaining())
                result.append((char) byteBuffer.getShort());

            return result.toString();
        }
    };

    public static void main(String[] args) {
        perform(FuncUtils.loadConfigFile(args[0]));
    }

    static void perform(JSONObject config) {
        String path = config.getString("path");
        String delim = config.getString("delim");
        int count = config.getInt("count");

        System.out.println("path: " + path);
        System.out.println("delim: " + delim);
        System.out.println("count: " + count);

        JSONObject lshParams = config.getJSONObject("lsh");

        int k = lshParams.getInt("k");
        int r = lshParams.getInt("r");
        int b = lshParams.getInt("b");


        long nBits;
        LSH<BaseSequence> oligoLSH;
        LSH<BaseSequence> addrLSH;
        Object lshType = lshParams.get("type");
        if (lshType.equals("bloom")) {
            nBits = lshParams.getLong("nBits");

            oligoLSH = BitMinHashLSH.newLSHForBaseSequences(k, r, nBits);
            addrLSH = BitMinHashLSH.newLSHForBaseSequences(k, r, nBits);
            System.out.println("LSH type: BloomLSH");
        }
        else if (lshType.equals("traditional")) {
            oligoLSH = MinHashLSH.newLSHForBaseSequences(k, r, b);
            addrLSH = MinHashLSH.newLSHForBaseSequences(k, r, b);
            System.out.println("LSH type: TraditionalLSH");
        }
        else {
            oligoLSH = MinHashLSHLight.newLSHForBaseSequences(k, r, b);
            addrLSH = MinHashLSHLight.newLSHForBaseSequences(k, r, b);
            System.out.println("LSH type: LightLSH");
        }

        JSONObject optimizations = config.getJSONObject("optimizations");
        JSONObject payloadOptimizations = optimizations.getJSONObject("payload");
        JSONObject addrOptimizations = optimizations.getJSONObject("address");
        int payloadPadding = payloadOptimizations.getInt("padding");
        int payloadPermutations = payloadOptimizations.getInt("permutations");


        int addressPermutations = addrOptimizations.getInt("permutations");

        String id = config.getString("id");

        System.out.println("id  : " + id);

        int addrSize = config.getInt("addrSize");
        int payloadSize = config.getInt("payloadSize");

        System.out.println("addressSize: " + addrSize + ", permutations: " + addressPermutations);
        System.out.println("payloadSize: " + payloadSize + ", padding: " + payloadPadding + ", permutations: " + payloadPermutations);


        var coder = parseCoder(config.getJSONObject("coder"));
        var translationOpts = config.getJSONObject("translation");
        var idTranslation = parseIdsContainer(translationOpts);
        var barcodesTranslation = parseBarcodesContainer(translationOpts, addrSize);
        calc(path, delim, coder, idTranslation.getT2(), idTranslation.getT1(), barcodesTranslation.getT2(), barcodesTranslation.getT1(), count, addrSize, addressPermutations, payloadSize, payloadPadding, payloadPermutations, id, oligoLSH, addrLSH);
    }

    private static Pair<Boolean, Container<Long, Long, Long>> parseIdsContainer(JSONObject translationOptions) {
        if (translationOptions.getBoolean("persistIds"))
            return new Pair<>(true, new PersistentContainer<>(translationOptions.getString("idsTranslationPath"), FixedSizeSerializer.LONG));

        return new Pair<>(false, Container.discardingContainer());
    }

    private static Pair<Boolean, Container<Long, BaseSequence, Long>> parseBarcodesContainer(JSONObject translationOptions, int addrSize) {
        if (translationOptions.getBoolean("persistBarcodes")) {
            if (addrSize % 4 != 0)
                throw new RuntimeException("address size must be divisible by 4 when using persistent storage for the translated barcodes! You used address size: " + addrSize);

            return new Pair<>(true, new PersistentContainer<>("barcodes.table", new FixedSizeSerializer<>() {
                @Override
                public int serializedSize() {
                    return 2 + addrSize / 4;
                }

                @Override
                public byte[] serialize(BaseSequence seq) {
                    return Packer.withBytePadding(SeqBitStringConverter.transform(seq)).toBytes();
                }

                @Override
                public BaseSequence deserialize(byte[] bs) {
                    return SeqBitStringConverter.transform(Packer.withoutBytePadding(new BitString(bs)));
                }
            }));
        }

        return new Pair<>(false, Container.discardingContainer());
    }

    private static Coder<String, BaseSequence> parseCoder(JSONObject jo) {
        var name = jo.getString("name");
        if (RotatingTre.INSTANCE.getClass().getSimpleName().equals(name))
            return RotatingTre.INSTANCE;
        if (RotatingQuattro.INSTANCE.getClass().getSimpleName().equals(name))
            return RotatingQuattro.INSTANCE;
        if (Bin.INSTANCE.getClass().getSimpleName().equals(name))
            return Bin.INSTANCE;

        else if (name.contains("fountain")) {
            DNARulesCollection packetRules = new DNARulesCollection();
            jo.getJSONArray("packetRules").forEach(ruleName -> packetRules.addRule(BasicDNARules.INSTANCE.getRules().get(ruleName.toString())));
            float maxPacketError = jo.getFloat("packetMaxError");

            DNARulesCollection strandRules = new DNARulesCollection();
            jo.getJSONArray("strandRules").forEach(ruleName -> strandRules.addRule(BasicDNARules.INSTANCE.getRules().get(ruleName.toString())));
            float maxStrandError = jo.getFloat("strandMaxError");

            Coder<byte[], BaseSequence> rq = new RQCoder(seq -> packetRules.evalErrorProbability(seq) <= maxPacketError, seq -> strandRules.evalErrorProbability(seq) <= maxStrandError, false);
            return Coder.fuse(BIJECTIVE_STRING_CODER, rq);
        }
            throw new RuntimeException("Invalid coder '" + name + "' not found");
    }

    public static void calc(String csvPath, String delim, Coder<String, BaseSequence> coder, Container<Long, Long, Long> idsContainer, boolean idsContainerIsPersistent, Container<Long, BaseSequence, Long> barcodesContainer, boolean barcodesContainerIsPersistent, long numEntries, int addressSize, int addressPermutations, int payloadSize, int payloadPadding, int payloadPermutations, String id, LSH<BaseSequence> oligoLSH, LSH<BaseSequence> lshAddrs) {

        AddressTranslationManager<Long, BaseSequence> atm = DNAAddrTranslationManager
                .builder()
                .setLsh(lshAddrs)
                .setAddrSize(addressSize)
                .setNumPermutations(addressPermutations)
                .setContainers(idsContainer, barcodesContainer, idsContainerIsPersistent, barcodesContainerIsPersistent)
                .build();

        SizedDNAContainer dnaContainer = SizedDNAContainer
                .builder()
                .setAddressTranslationManager(atm)
                .setOligoLSH(oligoLSH)
                .setPayloadSize(payloadSize)
                .setNumGcCorrectionsPayload(payloadPadding)
                .setNumPayloadPermutations(payloadPermutations)
                .build();

        Container<Long, String, BaseSequence> container = Container.transform(dnaContainer, coder);

        BufferedCsvReader reader = new BufferedCsvReader(csvPath, delim);

        AtomicReference<Long> numBytesPayload = new AtomicReference<>(0L);
        AtomicReference<Integer> numEntriesInserted = new AtomicReference<>(0);
        System.out.println("reading lines...");
        List<CsvLine> lines = reader.stream().limit(numEntries).toList();
        reader.close();
        System.out.println("inserting " + numEntries);

        long t1 = System.currentTimeMillis();
        lines.stream().parallel().forEach(line -> {
            var key = Long.parseLong(line.get(id));
            numBytesPayload.updateAndGet(v -> v + line.getLine().length());
            container.put(
                    key,
                    line.getLine());
            System.out.println("inserted " + numEntriesInserted.updateAndGet(v -> v + 1) + " entries into the container");
        });
        double timeInSecs = (System.currentTimeMillis() - t1) / 1000d;

        System.out.println("\ninsertion took: " + timeInSecs + " seconds -> " + numEntries / timeInSecs + " entry/sec");
        System.out.println("read data: " + (numBytesPayload.get() / 1000_000d) + " MBs");
    }
}
