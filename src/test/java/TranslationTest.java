import core.BaseSequence;
import datastructures.container.translation.DNAAddrManager;
import utils.lsh.LSH;
import utils.lsh.minhash.*;
import java.util.stream.LongStream;

public class TranslationTest {

    public static void main(String... args) {
        long count = 100L;
        int addressSize = 80;
        int numPerms = 8;
        int r = 5;
        int k = 5;

        LSH<BaseSequence> lsh = MinHashLSH.newSeqLSHTraditional(k, r);
        var atm = DNAAddrManager.builder()
                .setAddrSize(addressSize)
                .setLsh(lsh)
                .setNumPermutations(numPerms)
                .build();

        System.out.println("translating...");
        long t1 = System.currentTimeMillis();
        LongStream.range(0L, count).parallel().forEach(atm::routeAndTranslate);
        long t2 = System.currentTimeMillis();

        System.out.println("translation finished after " + (t2 - t1) / 1000f + " seconds");
    }
}
