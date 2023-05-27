package utils;

import java.util.function.Function;

/**
 * A coder that is not symmetric, i.e., the encode() method returns T, the decode() method returns I where T != I is possible.
 * @param <F> the type to encode.
 * @param <I> the decoded type.
 * @param <T> the encoded type.
 */
public interface AsymmetricCoder<F, I, T> extends Function<F, T> {
    /**
     * Encodes the input.
     * @param f the input.
     * @return the encoded input.
     */
    T encode(F f);

    /**
     * Decodes the input.
     * @param t the input.
     * @return the decoded input.
     */
    I decode(T t);

    @Override
    default T apply(F f) {
        return encode(f);
    }

    static <F, I, T> AsymmetricCoder<F, I, T> of (Function<F, T> encoder, Function<T, I> decoder) {
        return new AsymmetricCoder<>() {
            @Override
            public T encode(F f) {
                return encoder.apply(f);
            }
            @Override
            public I decode(T t) {
                return decoder.apply(t);
            }
        };
    }


    static <F1, T1, T2, I> AsymmetricCoder<F1, I, T2> fuse(AsymmetricCoder<F1, I, T1> ac1, AsymmetricCoder<T1, T1, T2> ac2) {
        return new AsymmetricCoder<>() {
            @Override
            public T2 encode(F1 f) {
                return ac2.encode(ac1.encode(f));
            }

            @Override
            public I decode(T2 t2) {
                return ac1.decode(ac2.decode(t2));
            }
        };
    }


    static <F1, T1, T2, T3, I> AsymmetricCoder<F1, I, T3> fuse(AsymmetricCoder<F1, I, T1> ac1, AsymmetricCoder<T1, T1, T2> ac2, AsymmetricCoder<T2, T2, T3> ac3) {
        return new AsymmetricCoder<>() {
            @Override
            public T3 encode(F1 f) {
                return ac3.encode(ac2.encode(ac1.encode(f)));
            }

            @Override
            public I decode(T3 t2) {
                return ac1.decode(ac2.decode(ac3.decode(t2)));
            }
        };
    }
}
