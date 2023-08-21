package utils.lsh.storage;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public interface LSHStorage<H> {
    void store(H hash);
    void remove(H hash);
    boolean query(H hash);

    class AmplifiedLSHStorage<H, S extends LSHStorage<H>> implements LSHStorage<H[]> {
        public enum Amplification {
            AND, OR
        }

        protected final List<S> bands;
        protected final Amplification amp;

        public AmplifiedLSHStorage(int numBands, Amplification amp, Supplier<S> supp) {
            this.bands = Stream.generate(supp).limit(numBands).toList();
            this.amp = amp;
        }

        public S band(int index) {
            return bands.get(index);
        }

        public Amplification amplification() {
            return amp;
        }

        public List<S> bands() {
            return bands;
        }

        @Override
        public void store(H[] hash) {
            IntStream.range(0, hash.length).forEach(i -> band(i).store(hash[i]));
        }

        @Override
        public void remove(H[] hash) {
            IntStream.range(0, hash.length).forEach(i -> band(i).remove(hash[i]));
        }

        @Override
        public boolean query(H[] hash) {
            return query(hash, amp);
        }

        public boolean query(H[] hash, Amplification amp) {
            IntStream range = IntStream.range(0, hash.length);
            return switch (amp) {
                case OR -> range.anyMatch(i -> band(i).query(hash[i]));
                case AND -> range.allMatch(i -> band(i).query(hash[i]));
            };
        }
    }
}
