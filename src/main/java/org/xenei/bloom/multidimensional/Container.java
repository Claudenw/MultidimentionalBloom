package org.xenei.bloom.multidimensional;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.apache.commons.collections4.bloomfilter.Hasher;

/**
 * A container that implements multidimensional Bloom filter storage.
 *
 * @param <E> The type of object being stored.
 */
public interface Container<E> {

    /**
     * Gets the number of values in the container.
     * @return the number of values in the container.
     */
    int getValueCount();

    /**
     * Gets the number of filters in the container.
     * @return the number of filters in the container.
     */
    int getFilterCount();

    /**
     * Gets the shape of the filters in the container.
     * @return the shape of the filters in the container.
     */
    Shape getShape();

    /**
     * Gets a stream of stored objects that have matching filters.
     * @param filter the filter to match.
     * @return a stream of stored objects
     */
    Stream<E> get(Hasher filter);

    /**
     * Puts an object into the container.
     * @param hasher a Hasher that generates hash values for the value.
     * @param value the value to store.
     */
    void put(Hasher hasher, E value);

    /**
     * Removes an object into the container.
     * Only stored values that have a Bloom filter exact match (bit by bit comparison) with
     * the Bloom filter created by the hasher will be removed.
     * @param hasher a Hasher that generates hash values for the value.
     * @param value the value to remove.
     */
    void remove(Hasher hasher, E value);

    /**
     * Searches the container for matching objects.
     * @param hasher the Hasher that generates hash values to create the Bloom filter to locate the values with.
     * @return a stream of stored objects that match the Bloom filter created by the hasher.
     */
    Stream<E> search(Hasher hasher);

    /**
     * A static method to create an empty stream.
     * @param <E> the type of object in the stream.
     * @return an empty stream.
     */
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
         * The value to return if the Bloom filter is not in the index.
         */
        static int NOT_FOUND = -1;

        /**
         * Get the index that matches the filter.
         *
         * @param hasher the hasher to match
         * @return the index that matches the filter or -1 if not found.
         */
        int get(Hasher hasher);

        /**
         * Put the bloom filter into the index. If the index already contains the filter
         * the operation is undefined.
         *
         * @param hasher the hasher to add
         * @return the index of the storage collection.
         */
        int put(Hasher hasher);

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
        /**
         * The index in the remove result that indicates the object was removed.
         */
        static final int REMOVED = 0;
        /**
         * The index in the remove result that indicates the result is now empty.
         */
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
         * @return first value true if the item was removed, second true if the storage
         *         index is empty after the removal.
         */
        boolean[] remove(int idx, E value);

        /**
         * Gets a stream of all the elements The indexes are not guaranteed to be in any
         * particular order. The lists are not guaranteed to be in any particular order.
         *
         * @return a stream of all the elements.
         */
        Stream<Map.Entry<Integer, List<E>>> list();
    }
}
