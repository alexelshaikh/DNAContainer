package core;

import utils.FuncUtils;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The Enum that represents a DNA base.
 */
public enum Base {
    A,
    T,
    C,
    G;

    /**
     * @return the complement for the given DNA base.
     */
    public Base complement() {
        return switch (this) {
            case A  -> T;
            case T  -> A;
            case C  -> G;
            default -> C;
        };
    }

    /**
     * @param c the character that is parsed to a DNA base.
     * @return the DNA base for c.
     */
    public static Base valueOfChar(char c) {
        return switch (c) {
            case 'A' -> A;
            case 'T' -> T;
            case 'G' -> G;
            case 'C' -> C;
            default  -> throw new RuntimeException("char not a valid base: " + c);
        };
    }

    public char ordinalAsChar() {
        return Character.forDigit(this.ordinal(), 4);
    }

    /**
     * @return a random DNA base.
     */
    public static Base random() {
        return randomGC(0.5d);
    }

    /**
     * @param gcContent the probability of returning a G or C.
     * @return a DNA base. The probability of returning a G or C is gcContent.
     */
    public static Base randomGC(double gcContent) {
        var randomGen = ThreadLocalRandom.current();
        double rand = randomGen.nextDouble();
        boolean isGorA = randomGen.nextBoolean();
        if (rand < gcContent)
            return isGorA ? Base.G : Base.C;

        return isGorA ? Base.A : Base.T;
    }

    /**
     * @param bs an array of DNA bases.
     * @return a random DNA base selected from bs.
     */
    public static Base random(Base... bs) {
        return FuncUtils.random(bs);
    }
}