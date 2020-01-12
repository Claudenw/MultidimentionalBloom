/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.bloom.multidimensional;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.collections4.bloomfilter.hasher.Hasher;
import org.apache.commons.collections4.bloomfilter.hasher.Shape;

/**
 * A container that implements multidimensional Bloom filter storage.
 *
 * @param <E> The type of object being stored.
 */
public interface Container<E> {

    /**
     * Gets the number of values in the container.
     *
     * @return the number of values in the container.
     */
    int getValueCount();

    /**
     * Gets the number of filters in the container.
     *
     * @return the number of filters in the container.
     */
    int getFilterCount();

    /**
     * Gets the shape of the filters in the container.
     *
     * @return the shape of the filters in the container.
     */
    Shape getShape();

    /**
     * Gets an iterator of stored objects that have matching filters.
     *
     * @param hasher the filter to match.
     * @return a stream of stored objects
     */
    Iterator<E> get(Hasher hasher);

    /**
     * Puts an object into the container.
     *
     * @param hasher a Hasher that generates hash values for the value.
     * @param value  the value to store.
     */
    void put(Hasher hasher, E value);

    /**
     * Removes an object into the container. Only stored values that have a Bloom
     * filter exact match (bit by bit comparison) with the Bloom filter created by
     * the hasher will be removed.
     *
     * @param hasher a Hasher that generates hash values for the value.
     * @param value  the value to remove.
     */
    void remove(Hasher hasher, E value);

    /**
     * Searches the container for matching objects.
     *
     * @param hasher the Hasher that generates hash values to create the Bloom
     *               filter to locate the values with.
     * @return an iterator of stored objects that match the Bloom filter created by
     *         the hasher.
     */
    Iterator<E> search(Hasher hasher);

    /**
     * A static method to create an empty stream.
     *
     * @param <E> the type of object in the stream.
     * @return an empty stream.
     */
    public static <E> Stream<E> emptyStream() {
        return Stream.empty();
    }

    /**
     * Internal storage. Stores the bloom object with the Bloom filter. More than
     * one object may be stored with a single Bloom filter.
     *
     * @param <I> The type of object used for the index.
     */
    public interface Index<I> {

        /**
         * Get the index that matches the filter.
         *
         * @param hasher the hasher to match
         * @return the index that matches the filter.
         */
        Optional<I> get(Hasher hasher);

        /**
         * Put the bloom filter into the index.
         *
         * @param idx the Index value to record the hasher at.
         * @param hasher the hasher to add
         * @return the index of the storage collection.
         */
        void put( I idx, Hasher hasher);

        /**
         * Remove the filter at the storage index from the index.
         *
         * @param index the index to remove.
         */
        void remove(I index);

        /**
         * Search for matching filters.
         *
         * @param hasher the hasher to search for.
         * @return the set of storage indexes.
         */
        Set<I> search(Hasher hasher);

        /**
         * Gets the number of filters indexed in the system.
         * @return The number of filters indexed in the system.
         */
        int getFilterCount();

        /**
         * Create the index value from the hasher
         * @param hasher the hasher to process
         * @return the index value.
         */
        I create(Hasher hasher);
    }

    /**
     * Internal storage. Stores the bloom object with the Bloom filter. More than
     * one object may be stored with a single Bloom filter.
     *
     * @param <E> the type of object.
     * @param <I> the type of the index object.
     */
    public interface Storage<E, I> {
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
        Collection<E> get(I idx);

        /**
         * Puts an object in the collection at the storage index.
         *
         * @param idx   the storage index.
         * @param value the value to put in the collection.
         */
        void put(I idx, E value);

        /**
         * Removes a value from the collection at the storage index
         *
         * @param idx   the index from which to remove the value.
         * @param value the value to remove
         * @return first value true if the item was removed, second true if the storage
         *         index is empty after the removal.
         */
        boolean[] remove(I idx, E value);

        /**
         * Gets a stream of all the elements The indexes are not guaranteed to be in any
         * particular order. The lists are not guaranteed to be in any particular order.
         *
         * @return a stream of all the elements.
         */
        Iterator<Map.Entry<I, List<E>>> list();
    }
}
