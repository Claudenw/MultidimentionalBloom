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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.xenei.bloom.filter.EWAHBloomFilter;
import org.xenei.bloom.multidimensional.Container.Index;
import com.googlecode.javaewah.datastructure.BitSet;

/**
 * An index that breaks the filter down by bytes and uses bitsets to determine which
 * bloom filters contain the specific byte value.
 *
 * The index contains an array of arrays of bitsets.  The array outer array is the
 * size of the number of bytes in the Bloom filter shape.
 * the inner array comprises 255 Bitsets, one for each value from 1 to 256.
 * The value 0 matches everything and is not stored as it provides no reduction
 * in scope.
 *
 * @param <I> The index type
 */
public class BlockedSetIndex<I> implements Index<I> {

    /**
     * The size of the chunks.
     */
    public static final int CHUNK_SIZE = Byte.SIZE;

    /**
     * The size of the block, number possible values.
     */
    private static final int BLOCK_SIZE= (1<<Byte.SIZE)-1;

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
     * The shape of the contained Bloom filters.
     */
    protected final Shape shape;
    /**
     * An array of blocks.
     */
    private final BitSet[][] list;

    /**
     * A list of index values matching bitset mapping.
     */
    private final List<I> values;

    /**
     * Function to convert BloomFilter to index.
     */
    private final Function<BloomFilter,I> func;

    /**
     * A bitset representing the free/deleted entries in the list/values.
     */
    private final BitSet empty;

    /**
     * The number of entries in the index.
     */
    private volatile int count;


    /**
     * Constructs a BlockedSetIndex.
     * Uses 1/shape.getProbability() as the estimated number of filters.
     * @param func the function to convert Bloom filter to index object.
     * @param shape the shape of the contained Bloom filters.
     */
    public BlockedSetIndex(Function<BloomFilter,I> func, Shape shape) {
        this.func = func;
        this.shape = shape;
        this.list = new BitSet[shape.getNumberOfBytes()][BLOCK_SIZE];
        this.empty = new BitSet(0);
        this.values = new ArrayList<I>();
        this.count = 0;
    }

    @Override
    public Optional<I> get(Hasher hasher) {
        EWAHBloomFilter filter = new EWAHBloomFilter( hasher, shape );
        long[] bits = filter.getBits();
        BitSet answer = null;
        List<Integer> removeList = new ArrayList<Integer>();
        for (int longIdx=0;longIdx<bits.length;longIdx++)
        {
            removeList.clear();
            int limit = Integer.min( Long.BYTES*(longIdx+1), shape.getNumberOfBytes());
            limit -= (Long.BYTES*longIdx);
            for (int byteIdx=0;byteIdx<limit;byteIdx++)
            {
                int blockIdx = longIdx*Long.BYTES+byteIdx;
                int bitIdx = (int)((bits[longIdx] >> byteIdx*Byte.SIZE) & MASK);
                // ignore 0 entries;
                if (bitIdx == 0)
                {
                    removeList.add( blockIdx );
                }
                else {
                    BitSet[] block = list[blockIdx];
                    if (block == null)
                    {
                        return Optional.empty();
                    }
                    // add the ones we are looking for
                    BitSet bitSet = block[bitIdx-1];
                    if (bitSet == null)
                    {
                        return Optional.empty();
                    } else  if (answer == null) {
                        answer = new BitSet( bitSet.size() );
                        answer.or( bitSet );
                    }
                    else {
                        answer.and( bitSet );
                        if (answer.empty())
                        {
                            return Optional.empty();
                        }
                    }
                }
            }
            // remove any
            for (int blockIdx : removeList )
            {
                BitSet[] block = list[blockIdx];
                if (block != null)
                {
                    for (BitSet bitSet : block )
                    {
                        if (bitSet != null)
                        {
                            answer.andNot( bitSet );
                        }
                    }
                }
            }
        }
        if (answer.cardinality() > 1) {
            throw new IllegalStateException();
        }
        return answer.nextSetBit(0)==-1 ? Optional.empty() : Optional.of( func.apply(filter));
    }

    @Override
    public I put(Hasher hasher) {
        int idx = empty.nextSetBit(-1);
        if (idx == -1) {
            idx = count++;
        } else {
            empty.unset(idx);
            if (empty.cardinality() == 0) {
                empty.resize(0);
            }
        }

        EWAHBloomFilter filter = new EWAHBloomFilter( hasher, shape );
        long[] bits = filter.getBits();
        for (int longIdx=0;longIdx<bits.length;longIdx++)
        {
            int limit = Integer.min( Long.BYTES*(longIdx+1), shape.getNumberOfBytes());
            limit -= (Long.BYTES*longIdx);
            for (int byteIdx=0;byteIdx<limit;byteIdx++)
            {
                int blockIdx = longIdx*Long.BYTES+byteIdx;
                int bitIdx = (int)((bits[longIdx] >> byteIdx*Byte.SIZE) & MASK);
                // ignore 0 entries;
                if (bitIdx != 0)
                {
                    BitSet[] block = list[blockIdx];
                    if (block == null)
                    {
                        block = new BitSet[BLOCK_SIZE];
                        list[blockIdx] = block;
                    }
                    BitSet bitSet = block[bitIdx-1];
                    if (bitSet == null)
                    {
                        bitSet = new BitSet( idx+1 );
                        block[ bitIdx-1 ] = bitSet;
                    } else  if (bitSet.size() < idx+1) {
                        bitSet.resize( bitSet.size()+64 );
                    }
                    bitSet.set(idx);
                }
            }
        }
        while (values.size() < idx+1)
        {
            values.add( null );
        }
        I result = func.apply( filter );
        values.set(idx, result);
        return result;
    }

    @Override
    public void remove(I idx) {
        int index = values.indexOf( idx );
        if (index > -1)
        {
            for (int blockIdx=0;blockIdx<list.length;blockIdx++)
            {
                BitSet[] block = list[blockIdx];
                if (block != null)
                {
                    boolean isEmpty = true;
                    for (int bitIdx=0;bitIdx<BLOCK_SIZE;bitIdx++)
                    {
                        BitSet bitSet = block[bitIdx];
                        if (bitSet != null)
                        {
                            bitSet.clear( index );
                            if (bitSet.empty())
                            {
                                block[bitIdx] = null;
                            } else
                            {
                                isEmpty = false;
                            }
                        }
                    }
                    if (isEmpty)
                    {
                        list[blockIdx] = null;
                    }
                }
            }
            if (empty.size() <= index) {
                empty.resize(index + 1);
            }
            empty.set(index);
            values.set(index, null);
        }
    }

    @Override
    public Set<I> search(Hasher hasher) {
        EWAHBloomFilter filter = new EWAHBloomFilter( hasher, shape );
        long[] bits = filter.getBits();
        BitSet answer = null;
        for (int longIdx=0;longIdx<bits.length;longIdx++)
        {
            int limit = Integer.min( Long.BYTES*(longIdx+1), shape.getNumberOfBytes());
            limit -= (Long.BYTES*longIdx);
            for (int byteIdx=0;byteIdx<limit;byteIdx++)
            {
                int listIdx = longIdx*Long.BYTES+byteIdx;
                int bitScan = (int)((bits[longIdx] >> byteIdx*Byte.SIZE) & MASK);
                if (bitScan != 0)
                {
                    BitSet union = null;

                    for (int bitIdx : byteTable[bitScan])
                    {
                        BitSet[] block = list[listIdx];
                        BitSet bitSet = block[bitIdx-1];
                        if (bitSet != null)
                        {
                            if (union == null) {
                                union = new BitSet( bitSet.size() );
                            }
                            union.or( bitSet );
                        }
                    }
                    if (union == null)
                    {
                        return Collections.emptySet();
                    }
                    if (answer == null)
                    {
                        answer = new BitSet( union.size() );
                        answer.or( union );
                    } else {
                        answer.and( union );
                        if (answer.empty())
                        {
                            return Collections.emptySet();
                        }
                    }
                }
            }
        }
        Set<I> result = new HashSet<I>();
        answer.iterator().forEachRemaining( i -> result.add( values.get(i)) );
        return result;
    }

}
