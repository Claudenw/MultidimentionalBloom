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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.collections4.bloomfilter.hasher.Hasher;
import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Shape;
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
     * A map of values to internal indexes
     */
    private final Map<I,Integer> valueToIdx;

    /**
     * Function to convert BloomFilter to index.
     */
    private final Function<BitMapProducer,I> func;

    /**
     * A bitset representing the free/deleted entries in the list/values.
     */
    private final BitSet empty;

    private static int numberOfBlocks(Shape shape) {
        return shape.getNumberOfBits() / CHUNK_SIZE + ((shape.getNumberOfBits() % CHUNK_SIZE) > 0?1:0);
    }

    private static int numberOfBytes(Shape shape) {
        return shape.getNumberOfBits() / Byte.BYTES + ((shape.getNumberOfBits() % Byte.BYTES) > 0?1:0);
    }

    /**
     * Constructs a BlockedSetIndex.
     * @param func the function to convert Bloom filter to index object.
     * @param shape the shape of the contained Bloom filters.
     */
    public BlockedSetIndex(Function<BitMapProducer,I> func, Shape shape) {
        this.func = func;
        this.shape = shape;
        this.list = new BitSet[numberOfBlocks(shape)][BLOCK_SIZE];
        this.empty = new BitSet(0);
        this.values = new ArrayList<I>();
        this.valueToIdx = new HashMap<I,Integer>();
    }

    @Override
    public Optional<I> get(Hasher hasher) {
        EWAHBloomFilter filter = new EWAHBloomFilter( shape, hasher );
        I result = func.apply(filter);
        return valueToIdx.containsKey( result ) ? Optional.of(result) : Optional.empty();
    }

    @Override
    public I put( Hasher hasher) {
        Optional<I> result = get(hasher);
        if (result.isEmpty()) {

            EWAHBloomFilter filter = new EWAHBloomFilter( shape, hasher );
            result = Optional.of( func.apply(filter) );
            int idx = empty.nextSetBit(-1);
            if (idx == -1) {
                idx = valueToIdx.size();
            } else {
                empty.unset(idx);
                if (empty.cardinality() == 0) {
                    empty.resize(0);
                }
            }

            BitMapProducer.ArrayBuilder builder = new BitMapProducer.ArrayBuilder( shape );
            filter.forEachBitMap( builder );
            long[] bitMap = builder.getArray();
            for (int longIdx=0;longIdx<bitMap.length;longIdx++)
            {
                int limit = Integer.min( Long.BYTES*(longIdx+1), numberOfBytes(shape));
                limit -= (Long.BYTES*longIdx);
                for (int byteIdx=0;byteIdx<limit;byteIdx++)
                {
                    int blockIdx = longIdx*Long.BYTES+byteIdx;
                    int bitIdx = (int)((bitMap[longIdx] >> byteIdx*Byte.SIZE) & MASK);
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
            values.set(idx, result.get());
            valueToIdx.put(result.get(), idx);
        }
        return result.get();
    }

    @Override
    public void remove(I idx) {
        Integer i = valueToIdx.get(idx);

        if (i != null)
        {
            int index = i.intValue();
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
            valueToIdx.remove( idx );
        }
    }

    @Override
    public Set<I> search(Hasher hasher) {
        EWAHBloomFilter filter = new EWAHBloomFilter( shape, hasher );
        BitMapProducer.ArrayBuilder builder = new BitMapProducer.ArrayBuilder( shape );
        filter.forEachBitMap( builder );
        long[] bitMap = builder.getArray();
        BitSet answer = null;
        for (int longIdx=0;longIdx<bitMap.length;longIdx++)
        {
            int limit = Integer.min( Long.BYTES*(longIdx+1), numberOfBytes(shape));
            limit -= (Long.BYTES*longIdx);
            for (int byteIdx=0;byteIdx<limit;byteIdx++)
            {
                int listIdx = longIdx*Long.BYTES+byteIdx;
                int bitScan = (int)((bitMap[longIdx] >> byteIdx*Byte.SIZE) & MASK);
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

    @Override
    public int getFilterCount() {
        return valueToIdx.size();
    }

    @Override
    public Shape getShape() {
        return shape;
    }

    @Override
    public Set<I> getAll() {
        return new HashSet<I>( values );
    }

}
