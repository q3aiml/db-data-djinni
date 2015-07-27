package net.q3aiml.dbdata.util;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class MoreCollectors {
    static final Set<Collector.Characteristics> CH_ID
            = Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.IDENTITY_FINISH));

    /**
     * version of toMap with fix for https://bugs.openjdk.java.net/browse/JDK-8040892 from java 9 ripped from
     * http://cr.openjdk.java.net/~plevart/jdk9-dev/Collectors.duplicateKey/webrev.02/src/share/classes/java/util/stream/Collectors.java.html
     */
    public static <T, K, U>
    Collector<T, ?, Map<K,U>> toMap(
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends U> valueMapper)
    {
        return new CollectorImpl<>(HashMap::new,
                uniqKeysMapAccumulator(keyMapper, valueMapper),
                uniqKeysMapMerger(),
                CH_ID);
    }

    /**
     * {@code BinaryOperator<Map>} that merges the contents of its right
     * argument into its left argument, throwing {@code IllegalStateException}
     * if duplicate keys are encountered.
     *
     * @param <K> type of the map keys
     * @param <V> type of the map values
     * @param <M> type of the map
     * @return a merge function for two maps
     */
    private static <K, V, M extends Map<K,V>>
    BinaryOperator<M> uniqKeysMapMerger() {
        return (m1, m2) -> {
            for (Map.Entry<K,V> e : m2.entrySet()) {
                K k = e.getKey();
                V v = Objects.requireNonNull(e.getValue());
                V u = m1.putIfAbsent(k, v);
                if (u != null) throw duplicateKeyException(k, u, v);
            }
            return m1;
        };
    }

    /**
     * {@code BiConsumer<Map, T>} that accumulates (key, value) pairs
     * extracted from elements into the map, throwing {@code IllegalStateException}
     * if duplicate keys are encountered.
     *
     * @param keyMapper a function that maps an element into a key
     * @param valueMapper a function that maps an element into a value
     * @param <T> type of elements
     * @param <K> type of map keys
     * @param <V> type of map values
     * @return an accumulating consumer
     */
    private static <T, K, V>
    BiConsumer<Map<K, V>, T> uniqKeysMapAccumulator(Function<? super T, ? extends K> keyMapper,
                                                    Function<? super T, ? extends V> valueMapper) {
        return (map, element) -> {
            K k = keyMapper.apply(element);
            V v = Objects.requireNonNull(valueMapper.apply(element));
            V u = map.putIfAbsent(k, v);
            if (u != null) throw duplicateKeyException(k, u, v);
        };
    }

    /**
     * Construct an {@code IllegalStateException} with appropriate message.
     *
     * @param k the duplicate key
     * @param u 1st value to be accumulated/merged
     * @param v 2nd value to be accumulated/merged
     */
    private static IllegalStateException duplicateKeyException(
            Object k, Object u, Object v) {
        return new IllegalStateException(String.format(
                "Duplicate key %s (attempted merging values %s and %s)",
                k, u, v));
    }

    @SuppressWarnings("unchecked")
    private static <I, R> Function<I, R> castingIdentity() {
        return i -> (R) i;
    }

    static class CollectorImpl<T, A, R> implements Collector<T, A, R> {
        private final Supplier<A> supplier;
        private final BiConsumer<A, T> accumulator;
        private final BinaryOperator<A> combiner;
        private final Function<A, R> finisher;
        private final Set<Characteristics> characteristics;

        CollectorImpl(Supplier<A> supplier,
                      BiConsumer<A, T> accumulator,
                      BinaryOperator<A> combiner,
                      Function<A,R> finisher,
                      Set<Characteristics> characteristics) {
            this.supplier = supplier;
            this.accumulator = accumulator;
            this.combiner = combiner;
            this.finisher = finisher;
            this.characteristics = characteristics;
        }

        CollectorImpl(Supplier<A> supplier,
                      BiConsumer<A, T> accumulator,
                      BinaryOperator<A> combiner,
                      Set<Characteristics> characteristics) {
            this(supplier, accumulator, combiner, castingIdentity(), characteristics);
        }

        @Override
        public BiConsumer<A, T> accumulator() {
            return accumulator;
        }

        @Override
        public Supplier<A> supplier() {
            return supplier;
        }

        @Override
        public BinaryOperator<A> combiner() {
            return combiner;
        }

        @Override
        public Function<A, R> finisher() {
            return finisher;
        }

        @Override
        public Set<Characteristics> characteristics() {
            return characteristics;
        }
    }
}
