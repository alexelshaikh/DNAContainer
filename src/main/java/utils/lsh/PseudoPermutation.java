package utils.lsh;

import java.util.concurrent.ThreadLocalRandom;

public class PseudoPermutation {

    private final long m; // num rows/elements (q grams)
    private final long p; // prime >= m
    private final long a; // random; 1 <= a <= p - 1
    private final long b; // random; 0 <= b <= p - 1

    /**
     * Creates a PseudoPermutation instance. This is an approximation for a real permutation, and is used to accelerate LSH.
     * @param m largest index for this instance to permute. For example, if you want to permute a 100 elements vector, m would be 100.
     * @param p_1 must be greater than or equal to m. This LSH will use the next prime number greater than p_1.
     */
    public PseudoPermutation(long m, long p_1) {
        if (p_1 < m)
            throw new RuntimeException("p (" + p_1 + ") must be >= m (" + m + ")");
        this.m = m;
        this.p = nextPrime(p_1);
        this.a = ThreadLocalRandom.current().nextLong(1L, p);
        this.b = ThreadLocalRandom.current().nextLong(0L, p);
    }

    public long getP() {
        return p;
    }

    private static long nextPrime(long start) {
        long p = start;
        if ((p & 1L) == 0L)
            p++;
        while (!isOddNumberAlsoPrime(p))
            p += 2L;

        return p;
    }

    /**
     * @return the permuted index of x.
     */
    public long apply(long x) {
        long y = ((a * x + b) % p) % m;
        while (y < 0L)
            y += m;
        return y;
    }

    private static boolean isOddNumberAlsoPrime(long odd) {
        long root = (long) Math.sqrt(odd) + 1L;
        for (long r = 3L; r <= root; r += 2L) {
            if (odd % r == 0L)
                return false;
        }
        return true;
    }
}
