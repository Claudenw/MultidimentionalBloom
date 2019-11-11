package org.xenei.bloom.multidimensional;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.apache.commons.collections4.bloomfilter.Hasher;

/**
 * The external interface.
 *
 * @param <E> The type of object being stored.
 */
public interface Container<E> {

    int getValueCount();
    int getFilterCount();

    Shape getShape();

    Stream<E> get(Hasher filter);

    void put(Hasher hasher, E value);

    void remove(Hasher hasher, E value);

    Stream<E> search(Hasher hasher);

    public static <E> Stream<E> emptyStream() {
        List<E> lst = Collections.emptyList();
        return lst.stream();
    }

    /**
     * Internal storage. Stores the bloom object with the Bloom filter. More than
     * one object may be stored with a single Bloom filter.
     *
     * @param <E> the type of object.
     */
    public interface Index {
        /**
         * Get the index that matches the filter.
         *
         * @param hasher the hasher to match
         * @return the index that matches the filter or -1 if not found.
         */
        int get(Hasher filter);

        /**
         * Put the bloom filter into the index. If the index already contains the filter
         * the operation is undefined.
         *
         * @param hasher the hasher to add
         * @return the index of the storage collection.
         */
        int put(Hasher filter);

        /**
         * Remove the filter at the storage index from the index.
         *
         * @param index the index to remove.
         */
        void remove(int index);

        /**
         * Search for matching filters.
         *
         * @param hasher the hasher to search for.
         * @return an iterator of storage indexes.
         */
        Stream<Integer> search(Hasher hasher);
    }

    /**
     * Internal storage. Stores the bloom object with the Bloom filter. More than
     * one object may be stored with a single Bloom filter.
     *
     * @param <E> the type of object.
     */
    public interface Storage<E> {
        static final int REMOVED = 0;
        static final int EMPTY = 1;

        /**
         * Gets the collection of objects at the storage index.
         *
         * @param idx the storage index.
         * @return a stream of E from the storage index.
         */
        Stream<E> get(int idx);

        /**
         * Puts an object in the collection at the storage index.
         *
         * @param idx   the storage index.
         * @param value the value to put in the collection.
         */
        void put(int idx, E value);

        /**
         * Removes a value from the collection at the storage index
         *
         * @param idx   the index from which to remove the value.
         * @param value the value to remove
         * @return first value true if the item was removed, second true if the storage index is empty after the removal.
         */
        boolean[] remove(int idx, E value);

        /**
         * Gets a stream of all the elements
         * The indexes are not guaranteed to be in any particular order.
         * The lists are not guaranteed to be in any particular order.
         * @return a stream of all the elements.
         */
        Stream<Map.Entry<Integer, List<E>>> list();
    }
}
