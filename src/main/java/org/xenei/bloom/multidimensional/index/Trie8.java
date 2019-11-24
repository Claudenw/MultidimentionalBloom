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
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.xenei.bloom.multidimensional.index.tri.Trie;

/**
 * A Trie index that uses 8 bit bytes as chunks.
 *
 * @see Trie
 */
public class Trie8 extends Trie {
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
     * @param shape the shape of the contained Bloom filters.
     */
    public Trie8(Shape shape) {
        super(Double.valueOf( 1.0/shape.getProbability() ).intValue(), shape, CHUNK_SIZE, MASK);
    }

    /**
     * Constructs a Trie8
     * @param the estimated number of Bloom filters to be indexed.
     * @param shape the shape of the contained Bloom filters.
     */
    public Trie8(int estimatedPopulation, Shape shape) {
        super(Double.valueOf( 1.0/shape.getProbability() ).intValue(), shape, CHUNK_SIZE, MASK);
    }


    @Override
    public int[] getNodeIndexes(int chunk) {
        return byteTable[chunk];
    }

}
