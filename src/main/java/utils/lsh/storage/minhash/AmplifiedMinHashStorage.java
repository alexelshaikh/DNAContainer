package utils.lsh.storage.minhash;

import utils.lsh.storage.LSHStorage;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class AmplifiedMinHashStorage<S extends LSHStorage<Long>> extends LSHStorage.AmplifiedLSHStorage<Long, S> {
    public AmplifiedMinHashStorage(int numBands, Amplification amp, Supplier<S> supp) {
        super(numBands, amp, supp);
    }

    public void store(long[] hash) {
        IntStream.range(0, hash.length).forEach(i -> band(i).store(hash[i]));
    }

    public boolean query(long[] hash, Amplification amp) {
        IntStream range = IntStream.range(0, hash.length);
        return switch (amp) {
            case OR -> range.anyMatch(i -> band(i).query(hash[i]));
            case AND -> range.allMatch(i -> band(i).query(hash[i]));
        };
    }

    public void remove(long[] hash) {
        IntStream.range(0, hash.length).forEach(i -> band(i).remove(hash[i]));
    }

    public static AmplifiedMinHashStorage<LightHashStorage<Long>> newAmplifiedLightMinHashStorage(int numBands, Amplification amp) {
        return new AmplifiedMinHashStorage<>(
                numBands,
                amp,
                LightHashStorage::new
        );
    }

    public static <O> AmplifiedMinHashStorage<TraditionalHashStorage<Long, O>> newAmplifiedTraditionalMinHashStorage(int numBands, Amplification amp) {
        return new AmplifiedMinHashStorage<>(
                numBands,
                amp,
                TraditionalHashStorage::new
        );
    }

    public static AmplifiedMinHashStorage<BloomFilterHashStorage<Long>> newAmplifiedBloomFilterMinHashStorage(int numBands, long numBits, long numHashFunctions, Amplification amp) {
        return new AmplifiedMinHashStorage<>(
                numBands,
                amp,
                () -> new BloomFilterHashStorage<>(numBits, numHashFunctions, Function.identity())
        );
    }
}
