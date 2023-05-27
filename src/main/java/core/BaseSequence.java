package core;

import utils.Permutation;
import utils.Streamable;
import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BaseSequence implements Streamable<Base>, Cloneable {

    public static final Collector<Base, BaseSequence, BaseSequence> COLLECTOR_BASE = Collector.of(BaseSequence::new, BaseSequence::append, (seq1, seq2) -> {seq1.append(seq2); return seq1;});
    public static final Collector<BaseSequence, BaseSequence, BaseSequence> COLLECTOR_SEQS = Collector.of(BaseSequence::new, BaseSequence::append, (seq1, seq2) -> {seq1.append(seq2); return seq1;});

    private static final int LOW_K_MER_LENGTH = 8;

    private List<Base> bases;
    private Map<String, Object> properties;

    /**
     * Creates an empty BaseSequence
     */
    public BaseSequence() {
        this.bases = new ArrayList<>();
    }

    /**
     * Creates a DNA sequence containing a list of provided DNA bases.
     * @param bases the list of DNA bases that will be added to this instance.
     */
    public BaseSequence(List<Base> bases) {
        this.bases = bases;
    }

    /**
     * Creates a DNA sequence containing the bases provided in the string.
     * @param seq the string of DNA bases that will be parsed and added to this instance.
     */
    public BaseSequence(String seq) {
        this();
        append(seq);
    }

    /**
     * Creates a DNA sequence containing the DNA sequences provided.
     * @param seqs the array of BaseSequence that will be each added into this instance.
     */
    public BaseSequence(BaseSequence... seqs) {
        int length = 0;
        for (BaseSequence seq : seqs)
            length += seq.length();

        this.bases = new ArrayList<>(length);
        for (BaseSequence s : seqs)
            this.bases.addAll(s.bases);
    }

    /**
     * Creates a DNA sequence containing an array of provided DNA bases.
     * @param bases the array of DNA bases that will be added to this instance.
     */
    public BaseSequence(Base... bases) {
        this.bases = new ArrayList<>(Arrays.asList(bases));
    }

    /**
     * Creates a new DNA sequence containing the input DNA sequence(s) in sequential order, i.e., a concatenation of the input.
     * @param seqs the input sequence(s).
     * @return a new DNA sequence representing the concatenation of the input DNA sequences.
     */
    public static BaseSequence join(BaseSequence... seqs) {
        return new BaseSequence(seqs);
    }

    /**
     * Computes the complement of this DNA sequence.
     * @return a new DNA sequence representing the complement of this instance.
     */
    public BaseSequence complement() {
        int len = length();
        List<Base> comp = new ArrayList<>(len);
        for (int i = 0; i < len; i++)
            comp.add(this.bases.get(i).complement());

        return new BaseSequence(comp);
    }

    /**
     * Inserts a base at the specified index.
     * @param index the index where the base will be inserted.
     * @param b the base that will be inserted.
     */
    public void insert(int index, Base b) {
        this.bases.add(index, b);
    }


    /**
     * Inserts a DNA sequence at the specified index.
     * @param index the index where the DNA sequence will be inserted.
     * @param seq the base that will be inserted.
     */
    public void insert(int index, BaseSequence seq) {
        this.bases.addAll(index, seq.bases);
    }

    /**
     * Appends a character representing a DNA base to this instance.
     * @param b the character representing a DNA base.
     */
    public void append(char b) {
        this.bases.add(Base.valueOfChar(b));
    }

    /**
     * Appends a DNA base to this instance.
     * @param b the DNA base.
     */
    public void append(Base b) {
        this.bases.add(b);
    }

    /**
     * Appends a DNA sequence to this instance.
     * @param seq the DNA sequence.
     */
    public void append(BaseSequence seq) {
        this.bases.addAll(seq.bases);
    }

    /**
     * Appends a CharSequence representing a DNA sequence to this instance.
     * @param charSequence the CharSequence representing a DNA sequence.
     */
    public void append(CharSequence charSequence) {
        int len = charSequence.length();
        for(int i = 0; i < len; i++)
            append(charSequence.charAt(i));
    }

    /**
     * Replaces a DNA base at the given position.
     * @param index the position to set.
     * @param b the DNA base to set.
     */
    public void set(int index, Base b) {
        this.bases.set(index, b);
    }


    /**
     * Puts a property to this instance.
     * @param propertyName the property name.
     * @param value the property's value.
     */
    public <T> BaseSequence putProperty(String propertyName, T value) {
        if (properties == null)
            properties = new HashMap<>();
        properties.put(propertyName, value);
        return this;
    }

    /**
     * @param propertyName the property name.
     * @return the value of the given property.
     */
    public <T> T getProperty(String propertyName) {
        return getProperty(propertyName, () -> null);
    }

    /**
     * @param propertyName the property name.
     * @param orElse is the given property does not exist, will return this instead.
     * @return the value of the given property. If no value exists, returns orElse instead.
     */
    public <T> T getProperty(String propertyName, Supplier<T> orElse) {
        if (properties == null)
            return orElse.get();

        T t = (T) properties.get(propertyName);
        return t != null? t : orElse.get();
    }

    /**
     * @param len the k-mer length.
     * @return the list of k-mers (can contain duplicates).
     */
    public List<BaseSequence> kmers(int len) {
        int thisLen = length();
        if (len > thisLen)
            throw new RuntimeException("cannot create q grams of len " + len + " for seq of len " + thisLen);

        int sizeLimit = 1 + thisLen - len;
        List<BaseSequence> qGrams = new ArrayList<>(sizeLimit);
        for (int i = 0; i < sizeLimit; i++)
            qGrams.add(window(i, i + len));

        return qGrams;
    }

    /**
     * Returns all properties for this instance.
     * @return the map of properties.
     */
    public Map<String, Object> getProperties() {
        return properties;
    }


    /**
     * @return the number of DNA bases in this instance.
     */
    public int length() {
        return this.bases.size();
    }

    /**
     * Returns the underlying Base list.
     * @return the List of bases.
     */
    public List<Base> getBases() {
        return bases;
    }

    /**
     * Returns new BaseSequence representing this instance but replaced source with target.
     * @param source the DNA sequence to be replaced.
     * @param target the DNA sequence that replaces source
     * @return a new instance where source is replaced with target.
     */
    public BaseSequence replace(BaseSequence source, BaseSequence target) {
        return clone().replaceInPlace(source, target);
    }

    /**
     * Replaces source with target in this instance in-place.
     * @param source the DNA sequence to be replaced.
     * @param target the DNA sequence that replaces source
     * @return this instance where source is replaced with target.
     */
    public BaseSequence replaceInPlace(BaseSequence source, BaseSequence target) {
        int index = Collections.indexOfSubList(bases, source.bases);
        if (index >= 0) {
            BaseSequence before = index == 0? new BaseSequence() : window(0, index);
            BaseSequence after = window(index + source.length());
            List<Base> basesReplaced = new ArrayList<>(before.length() + target.length() + after.length());
            basesReplaced.addAll(before.bases);
            basesReplaced.addAll(target.bases);
            basesReplaced.addAll(after.bases);
            this.bases = basesReplaced;
        }
        return this;
    }

    /**
     * Calculate a seed for this instance. Two instances that are equal returns the same seed. Two instances that are not equal likely return different seeds.
     * @return the seed for this DNA sequence.
     */
    public long seed() {
        return histogram().values().stream().reduce(1, (x, y) -> x * y);
    }

    /**
     * Divides this instance in n-mers every n bases. If length() % n != 0, then the last split is smaller than n.
     * @param n the length at which the split happens.
     * @return the DNA sequence array containing the splits.
     */
    public BaseSequence[] splitEvery(int n) {
        int length = length();
        if (n > length)
            return new BaseSequence[] {this};

        BaseSequence[] split = new BaseSequence[(int) Math.ceil((double) length / n)];
        int i = 0;
        int start = 0;
        for (int end = n; end <= length;) {
            split[i++] = subSequence(start, end);
            start = end;
            end += n;
        }
        if (split[split.length - 1] == null)
            split[split.length - 1] = subSequence(start, length);

        return split;
    }

    /**
     * @return a new DNA sequence representing the reversed DNA sequence.
     */
    public BaseSequence reverse() {
        int len = bases.size();
        List<Base> reversed = new ArrayList<>(len);
        for (int i = len - 1; i >= 0; i--)
            reversed.add(bases.get(i));

        return new BaseSequence(reversed);
    }

    /**
     * Reverses this instance in-place.
     * @return the reversed DNA sequence.
     */
    public BaseSequence reverseInPlace() {
        int len = length();
        int lastIndex = len - 1;
        int lenHalf = len / 2;
        for (int i = 0; i < lenHalf; i++)
            swap(i, lastIndex - i);

        return this;
    }

    /**
     * Returns the last index at which this instance matches a given DNA sequence.
     * @param seq the sequence to search for.
     * @return the last index at which this instance matches seq in this instance.
     */
    public int lastIndexOf(BaseSequence seq) {
        return Collections.lastIndexOfSubList(this.bases, seq.bases);
    }


    /**
     * @return an iterator representing the DNA bases of this instance.
     */
    @Override
    public Iterator<Base> iterator() {
        return bases.listIterator();
    }

    /**
     * Swaps the two DNA bases at the given indexes in-place.
     * @param i the first index.
     * @param j the second index.
     */
    public void swap(int i, int j) {
        Base bi = this.bases.get(i);
        this.bases.set(i, this.bases.get(j));
        this.bases.set(j, bi);
    }

    /**
     * @return the absolute number of G and C in this instance.
     */
    public int gcCount() {
        return (int) stream().filter(b -> b == Base.C || b == Base.G).count();
    }

    /**
     * @return a map containing the absolute number of each DNA base.
     */
    public Map<Base, Integer> histogram() {
        return stream().collect(Collectors.toMap(b -> b, b -> 1, Integer::sum));
    }

    /**
     * Returns a new DNA sequence that is a subsequence of this instance.
     * @param i the starting (inclusive) index.
     * @param j the ending (exclusive) index.
     * @return the subsequence at indexes [i..j) of this instance.
     */
    public BaseSequence subSequence(int i, int j) {
        return new BaseSequence(new ArrayList<>(this.bases.subList(i, j)));
    }

    /**
     * Returns a new DNA sequence that is a subsequence of this instance.
     * @param i the starting (inclusive) index.
     * @return the subsequence at indexes [i..length()) of this instance.
     */
    public BaseSequence subSequence(int i) {
        return subSequence(i, length());
    }

    /**
     * Returns an immutable subsequence of this instance.
     * @param i the starting (inclusive) index.
     * @param j the ending (exclusive) index.
     * @return the subsequence at indexes [i..j) of this instance.
     */
    public BaseSequence window(int i, int j) {
        return new BaseSequence(this.bases.subList(i, j));
    }

    /**
     * Returns an immutable subsequence of this instance.
     * @param i the starting (inclusive) index.
     * @return the subsequence at indexes [i..length()) of this instance.
     */
    public BaseSequence window(int i) {
        return new BaseSequence(this.bases.subList(i, length()));
    }

    /**
     * Checks if a given DNA sequence is contained in this instance.
     * @param seq the DNA sequence to search.
     * @return true if seq is found in this instance, and false otherwise.
     */
    public boolean contains(BaseSequence seq) {
        return Collections.indexOfSubList(this.bases, seq.bases) >= 0;
    }

    /**
     * @return the longest homopolymer length of this instance.
     */
    public int longestHomopolymer() {
        int longest = 1;
        int current = 1;
        int len = length();
        for (int i = 1; i < len; i++) {
            if (get(i) == get(i - 1)) {
                current++;
            }
            else {
                longest = Math.max(longest, current);
                current = 1;
            }
        }
        return Math.max(longest, current);
    }


    /**
     * Searches for homopolymers that are longer than a specified threshold.
     * @param threshold the minimum homopolymer length.
     * @return an int array containing the indexes for the found homopolymers that are longer than threshold.
     */
    public int[] indexOfHomopolymersAboveThreshold(int threshold) {
        int limit = length() - threshold;
        IntStream.Builder indexes = IntStream.builder();
        int hpLen;
        int i = 0;
        while (i < limit) {
            hpLen = lengthOfHomopolymerAtIndex(i);
            if (hpLen > threshold)
                indexes.add(i);

            i += hpLen;
        }
        return indexes.build().toArray();
    }

    /**
     * Calculates the length of the homopolymer starting at a specified position.
     * @param index the index of the homopolymer.
     * @return the length of the homopolymer starting at index.
     */
    public int lengthOfHomopolymerAtIndex(int index) {
        int len = length();
        Base hpBase = this.bases.get(index);
        int hpLen = 0;
        while(index < len && this.bases.get(index++) == hpBase)
            hpLen++;

        return hpLen;
    }

    /**
     * Returns a new DNA sequence representing the permuted DNA sequence of this instance.
     * @param p the permutation.
     * @return the permuted DNA sequence.
     */
    public BaseSequence permute(Permutation p) {
        return clone().permuteInPlace(p);
    }

    /**
     * Permutes this instance in-place with a given permutation.
     * @param p the permutation.
     * @return this instance after permutation.
     */
	public BaseSequence permuteInPlace(Permutation p) {
        p.applyInPlace(this.bases);
        return this;
    }

    /**
     * @return the gc content of this instance.
     */
	public float gcContent() {
		return gcContentOf(bases);
    }

    /**
     * @param list the DNA bases list.
     * @return the gc content of the given list.
     */
    private static float gcContentOf(List<Base> list) {
        return (float) list.stream().filter(b -> b == Base.G || b == Base.C).count() / list.size();
    }

    /**
     * Returns the gc content of the specified window.
     * @param i the start (inclusive) index.
     * @param j the end (exclusive) index.
     * @return the gc content of this instance in [i, j).
     */
    public float gcWindow(int i, int j) {
        return gcContentOf(bases.subList(i, Math.min(j, length())));
    }


    /**
     * @param slice the DNA sequence.
     * @param consecutive if set true, then only consecutive repeats of slice will be counted.
     * @return the count of slice in this instance.
     */
    public int countMatches(BaseSequence slice, boolean consecutive) {
        int lenThis = length();
        int sliceLen = slice.length();
        if (lenThis < sliceLen)
            return 0;

        int start = 0;
        int end = sliceLen;
        int count = 0;
        int maxConsecutiveCount = 0;
        int consecutiveCount = 0;

        while (end < lenThis) {
            if (bases.subList(start, end).equals(slice.bases)) {
                count += 1;
                consecutiveCount += 1;
                start += sliceLen;
                end += sliceLen;
            } else {
                consecutiveCount = 0;
                start += 1;
                end += 1;
            }
            maxConsecutiveCount = Math.max(maxConsecutiveCount, consecutiveCount);
        }

        return consecutive? maxConsecutiveCount : count;
    }

    /**
     * @param i the index.
     * @return the DNA base at the specified index.
     */
    public Base get(int i) {
        return this.bases.get(i);
    }


    /**
     * Checks if this instance is equal to o.
     * @param o the other object.
     * @return true, if o and this instance contains the same DNA bases in the same order.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null)
            return false;
        if (o instanceof BaseSequence seq)
            return this.bases.equals(seq.bases);
        if (o instanceof String s)
            return length() == s.length() && toString().equals(s);

        return false;
    }

    /**
     * @return a hash value for this instance.
     */
    @Override
    public int hashCode() {
        return this.bases.hashCode();
    }

    /**
     * Generates a random BaseSequence with given length and GC content.
     * @param len the length of the returned BaseSequence.
     * @param gcContent the target GC content.
     * @return the random BaseSequence.
     */
    public static BaseSequence random(int len, double gcContent) {
        return new BaseSequence(Stream.generate(() -> Base.randomGC(gcContent)).limit(len).collect(Collectors.toList()));
    }

    /**
     * Generates a random BaseSequence.
     * @param len the length of the returned BaseSequence.
     * @return the random BaseSequence.
     */
    public static BaseSequence random(int len) {
        return random(len, 0.5d);
    }

    /**
     * Computes the longest common sequence's length.
     * @param seq the other BaseSequence.
     * @return the longest common sequence's length of this instance and the given one.
     */
    public int lcs(BaseSequence seq){
        int m = length();
        int n = seq.length();

        int max = 0;
        int[][] dp = new int[m][n];

        for (int i = 0; i < m && m - i > max; i++) {
            for (int j = 0; j < n && n - j > max; j++) {
                if (get(i) == seq.get(j)) {
                    if (i==0 || j==0)
                        dp[i][j]=1;
                    else
                        dp[i][j] = dp[i-1][j-1]+1;

                    if (max < dp[i][j])
                        max = dp[i][j];
                }
            }
        }
        return max;
    }

    /**
     * Returns the Hamming distance to the input BaseSequence.
     * @param seq the other BaseSequence.
     * @return the Hamming distance of this instance to the given BaseSequence.
     */
    public float hammingDistance(BaseSequence seq) {
        int len = this.length();
        int thatLen = seq.length();
        int minLen = Math.min(len, thatLen);
        float dist = Math.abs(len - thatLen);
        for (int i = 0; i < minLen; i++) {
            if (seq.get(i) != get(i))
                dist++;
        }

        return dist / Math.max(len, thatLen);
    }

    /**
     * Returns the normalized [0, 1] edit distance to the input BaseSequence.
     * @param seq the other BaseSequence.
     * @return the normalized edit distance of this instance to the given BaseSequence.
     */
    public float editDistance(BaseSequence seq) {
        int maxLen = Math.max(length(), seq.length());
        return levenshteinDistance(seq, maxLen) / maxLen;
    }

    /**
     * Returns the edit distance, i.e., the numer of edits to the input BaseSequence.
     * @param seq the other BaseSequence.
     * @return the edit distance of this instance to the given BaseSequence.
     */
    public int editDistanceNotNormalized(BaseSequence seq) {
        int maxLen = Math.max(length(), seq.length());
        return (int) levenshteinDistance(seq, maxLen);
    }

    private float levenshteinDistance(BaseSequence seq, int limit) {
        int len = length();
        int seqLen = seq.length();
        if (len == 0)
            return seqLen;
        if (seqLen == 0)
            return len;

        int[] v0 = new int[seqLen + 1];
        int[] v1 = new int[seqLen + 1];
        int[] vtemp;

        for (int i = 0; i < v0.length; i++)
            v0[i] = i;

        for (int i = 0; i < len; i++) {
            v1[0] = i + 1;
            int minv1 = v1[0];
            for (int j = 0; j < seqLen; j++) {
                int cost = 1;
                if (get(i) == seq.get(j)) {
                    cost = 0;
                }
                v1[j + 1] = Math.min(v1[j] + 1, Math.min(v0[j + 1] + 1, v0[j] + cost));
                minv1 = Math.min(minv1, v1[j + 1]);
            }

            if (minv1 >= limit)
                return limit;
            vtemp = v0;
            v0 = v1;
            v1 = vtemp;
        }

        return v0[seqLen];
    }


    /**
     * Returns the Jaccard distance to the input BaseSequence given a k-mer length.
     * @param seq the other BaseSequence.
     * @param k the k-mer length.
     * @return the Jaccard distance of this instance to the given BaseSequence.
     */
    public float jaccardDistance(BaseSequence seq, int k) {
        if (k <= LOW_K_MER_LENGTH)
            return jaccardDistanceLowK(kmersJaccard(this, k), kmersJaccard(seq, k));

        Set<BaseSequence> s1 = new HashSet<>(this.kmers(k));
        Set<BaseSequence> s2 = new HashSet<>(seq.kmers(k));
        Set<BaseSequence> union = new HashSet<>(s1);
        union.addAll(s2);

        s1.retainAll(s2);
        return 1.0f - (float) s1.size() / union.size();
    }

    private static BitSet kmersJaccard(BaseSequence seq, int k) {
        BitSet bs = new BitSet();
        for (BaseSequence km : seq.kmers(k))
            bs.set((int) km.toBase4());

        return bs;
    }

    private static float jaccardDistanceLowK(BitSet km1, BitSet km2) {
        BitSet intersectBitSet = (BitSet) km1.clone();
        intersectBitSet.and(km2);
        km1.or(km2);

        return 1.0f - intersectBitSet.cardinality() / (float) km1.cardinality();
    }

    /**
     * Returns the weighted Jaccard distance to the input BaseSequence given a k-mer length.
     * @param seq the other BaseSequence.
     * @param k the k-mer length.
     * @return the weighted Jaccard distance of this instance to the given BaseSequence.
     */
    public float jaccardDistanceWeighted(BaseSequence seq, int k) {
        if (k <= 6)
            return jaccardDistanceWeightedLowK(seq, k);

        Map<BaseSequence, Long> counts1 = this.kmers(k).stream().collect(Collectors.groupingBy(km -> km, Collectors.counting()));
        Map<BaseSequence, Long> counts2 = seq.kmers(k).stream().collect(Collectors.groupingBy(km -> km, Collectors.counting()));

        Set<BaseSequence> kmers = new HashSet<>(counts1.keySet());
        kmers.addAll(counts2.keySet());
        long intersects = 0L;
        long unions = 0L;
        long c1;
        long c2;
        for (BaseSequence kmer : kmers) {
            c1 = counts1.getOrDefault(kmer, 0L);
            c2 = counts2.getOrDefault(kmer, 0L);
            if (c1 <= c2) {
                intersects += c1;
                unions += c2;
            }
            else {
                intersects += c2;
                unions += c1;
            }
        }

        return 1.0f - intersects / (float) unions;
    }

    private float jaccardDistanceWeightedLowK(BaseSequence seq, int k) {
        int intersects = 0;
        int unions = 0;
        int kmersCount = (int) Math.pow(4, k);
        int[] km1 = this.kmersWeighted(k, kmersCount);
        int[] km2 = seq.kmersWeighted(k, kmersCount);
        int km1Item;
        int km2Item;
        for (int i = 0; i < km1.length; i++) {
            km1Item = km1[i];
            km2Item = km2[i];
            if (km1Item <= km2Item) {
                intersects += km1Item;
                unions += km2Item;
            }
            else {
                intersects += km2Item;
                unions += km1Item;
            }
        }


        return 1.0f - intersects / (float) unions;
    }


    private int[] kmersWeighted(int k, int max) {
        int[] kmerArray = new int[max];
        this.kmers(k).stream().mapToLong(BaseSequence::toBase4).forEach(index -> kmerArray[(int) index] += 1);
        return kmerArray;
    }

    /**
     * Converts the DNA sequence to a string representing the DNA bases.
     * @return a string representation of this instance.
     */
    @Override
    public String toString() {
        return stream().map(Base::name).collect(Collectors.joining());
    }

    /**
     * @return a copy of this instance.
     */
    @Override
    public BaseSequence clone() {
        return new BaseSequence(this);
    }


    /**
     * Converts this BaseSequence to a number in base 4.
     * @return the base 4 representation of this BaseSequence
     */
    public long toBase4() {
        int len = length();
        long id = 0L;
        long order;
        for (int i = 0; i < len; i++) {
            order = switch (this.bases.get(i)) {
                case A -> 0L;
                case C -> 1L;
                case G -> 2L;
                case T -> 3L;
            };
            if (order == 0L)
                continue;
            id += order * Math.pow(4.0d, i);
        }

        return id;
    }
}
