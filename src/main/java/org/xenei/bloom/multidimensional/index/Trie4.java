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
package org.xenei.bloom.multidimensional.index;

import java.util.function.Function;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.hasher.Shape;
import org.xenei.bloom.multidimensional.index.tri.Trie;

/**
 * A Trie index that uses 4 bit nibbles as chunks.
 * <ul>
 * <li>m = number of bits in the bloom filter</li>
 * <li>N = number of unique filters stored in the trie.</li>
 * <li>Insert costs: O( m/4 )</li>
 * <li>Search costs: O( 1.5^4 * m/4 ) = O( 5.0625 * m/4 )
 * <li>Memory requirements: O(2^4 * m/4 * N) = O(4Nm)</li>
 * </ul>
 * @see Trie
 * @param <I> The index type
 */
public class Trie4<I> extends Trie<I> {

    /**
     * The chunk size.
     */
    private static final int CHUNK_SIZE = 4;

    /**
     * the bit mask.
     */
    private static final long MASK = 0x0fL;

    /**
     * A list of nibbles to matching nibbles in the bloom filter.
     */
    private static final int[][] nibbleTable = { { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF },
            { 1, 3, 5, 7, 9, 0xB, 0xD, 0xF }, { 2, 3, 6, 7, 0xA, 0xB, 0xE, 0xF }, { 3, 7, 0xB, 0xF },
            { 4, 5, 6, 7, 0xC, 0xD, 0xE, 0xF }, { 5, 7, 0xD, 0xF }, { 6, 7, 0xE, 0xF }, { 7, 0xF },
            { 8, 9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF }, { 9, 0xB, 0xD, 0xF }, { 0xA, 0xB, 0xE, 0xF }, { 0xB, 0xF },
            { 0xC, 0xD, 0xE, 0xF }, { 0xD, 0xF }, { 0xE, 0xF }, { 0xF }, };

    /**
     * Constructs a Trie4.
     * Uses 1/shape.getProbability() as the estimated number of filters.
     * @param func the function to convert Bloom filter to index object.
     * @param shape the shape of the contained Bloom filters.
     */
    public Trie4(Function<BloomFilter,I> func, Shape shape) {
        super(func, Double.valueOf( 1.0/shape.getProbability() ).intValue(), shape, CHUNK_SIZE, MASK);
    }
    /**
     * Constructs a Trie4.
     * @param func the function to convert Bloom filter to index object.
     * @param estimatedPopulation the estimated number of Bloom filters to index.
     * @param shape the shape of the contained Bloom filters.
     */
    public Trie4(Function<BloomFilter,I> func, int estimatedPopulation, Shape shape) {
        super(func, estimatedPopulation, shape, CHUNK_SIZE, MASK);
    }

    @Override
    public int[] getNodeIndexes(int chunk) {
        return nibbleTable[chunk];
    }

}
