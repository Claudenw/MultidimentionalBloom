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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.xenei.bloom.multidimensional.index.tri.Trie;

/**
 * A Trie index that uses 8 bit bytes as chunks.
 * <ul>
 * <li>m = number of bits in the bloom filter</li>
 * <li>N = number of unique filters stored in the trie.</li>
 * <li>Insert costs: O( m/8 )</li>
 * <li>Search costs: O( 1.5^8 * m/8 ) = O( 25.6289 * m/8 )
 * <li>Memory requirements: O(2^8 * m/8 * N) = O(32Nm)</li>
 * </ul>
 * @see Trie
 * @param <I> The index type
 */
public class Trie8<I> extends Trie<I> {
    /**
     * The size of the chunks.
     */
    public static final int CHUNK_SIZE = Byte.SIZE;

    /**
     * The mask for the chunks
     */
    public static final long MASK = 0xFFL;

    /**
     * A list of bytes to matching bytes in the bloom filter.
     */
    private static final int[][] byteTable;


    static {
        // populate the byteTable
        int limit = (1 << CHUNK_SIZE);
        byteTable = new int[limit][];
        List<Integer> lst = new ArrayList<Integer>();

        for (int i = 0; i < limit; i++) {
            for (int j = 0; j < limit; j++) {
                if ((j & i) == i) {
                    lst.add(j);
                }
            }
            byteTable[i] = lst.stream().mapToInt(Integer::intValue).toArray();
            lst.clear();
        }

    }

    /**
     * Constructs a Trie8.
     * Uses 1/shape.getProbability() as the estimated number of filters.
     * @param func the function to convert Bloom filter to index object.
     * @param shape the shape of the contained Bloom filters.
     */
    public Trie8(Function<BloomFilter,I> func, Shape shape) {
        super(func, Double.valueOf( 1.0/shape.getProbability() ).intValue(), shape, CHUNK_SIZE, MASK);
    }

    /**
     * Constructs a Trie8
     * @param func the function to convert Bloom filter to index object.
     * @param the estimated number of Bloom filters to be indexed.
     * @param shape the shape of the contained Bloom filters.
     */
    public Trie8(Function<BloomFilter,I> func, int estimatedPopulation, Shape shape) {
        super(func, Double.valueOf( 1.0/shape.getProbability() ).intValue(), shape, CHUNK_SIZE, MASK);
    }


    @Override
    public int[] getNodeIndexes(int chunk) {
        return byteTable[chunk];
    }

}
