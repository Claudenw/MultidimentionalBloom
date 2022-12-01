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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.xenei.bloom.filter.EWAHBloomFilter;
import org.xenei.bloom.filter.HasherCollection;
import org.xenei.bloom.multidimensional.Container.Index;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah.IntIterator;
import com.googlecode.javaewah.datastructure.BitSet;

/**
 * This is what Daniel called Bloofi2. Basically, instead of using a tree
 * structure like Bloofi (see BloomFilterIndex), we "transpose" the BitSets.
 *
 * Originally from
 * https://github.com/lemire/bloofi/blob/master/src/mvm/provenance/FlatBloomFilterIndex.java
 *
 * @param <I> The index type
 */
public final class FlatBloofi<I> implements Index<I> {

    /**
     * The shape of the bloom filters.
     */
    private final Shape shape;

    /**
     * A list of buffers. Each entry in the buffer is 64 filters. Each long in the
     * long[] is a bit in the bloom filter.
     */
    private ArrayList<long[]> buffer;

    /**
     * A bitset that indicates which entries are in use. entry/64 = buffer index.
     */
    private BitSet busy;

    /**
     * A list of values.
     */
    private final List<I> values;

    /**
     * Function to convert Hasher to index.
     */
    private final Function<BitMapProducer, I> func;

    /**
     * Constructs a flat bloofi.
     * 
     * @param func the function to convert Bloom filter to index object.
     * @param shape the Shape of the contained Bloom filters.
     */
    public FlatBloofi(Function<BitMapProducer, I> func, Shape shape) {
        this.func = func;
        this.shape = shape;
        this.buffer = new ArrayList<long[]>();
        this.busy = new BitSet(0);
        this.values = new ArrayList<I>();
    }

    /**
     * Clear (remove) the bloom filter from the buffers
     * 
     * @param idx the index of the bloom filter in the busy bit set.
     */
    private void clearBloomAt(int idx) {
        final long[] mybuffer = buffer.get(idx / 64);
        final long mask = ~(1l << idx);
        for (int k = 0; k < mybuffer.length; ++k) {
            mybuffer[k] &= mask;
        }
    }

    /**
     * Using the hasher set the bits for a bloom filter.
     * 
     * @param idx the index of the bloom filter in the busy set.
     * @param hasher the hasher to generate the bits to turn on.
     */
    private void setBloomAt(int idx, BloomFilter filter) {
        final long[] mybuffer = buffer.get(idx / 64);
        final long mask = (1l << idx);
        filter.forEachIndex(i -> {
            mybuffer[i] |= mask;
            return true;
        });
    }

    @Override
    public Optional<I> get(HasherCollection hashers) {
        BitSet answer = new BitSet(busy.size());
        answer.or(busy);
        EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
        EWAHBloomFilter filter = new EWAHBloomFilter(shape, bitmap);
        hashers.fill(filter);

        for (int bufferNumber = 0; bufferNumber < buffer.size(); ++bufferNumber) {
            if (answer.cardinality() == 0) {
                return Optional.empty();
            }
            long[] buf = buffer.get(bufferNumber);
            for (int bitIdx = 0; bitIdx < buf.length; bitIdx++) {
                long l = buf[bitIdx];
                if (bitmap.get(bitIdx)) {
                    l &= 0xffffffffffffffffL;
                } else {
                    l ^= 0xffffffffffffffffL;
                }
                for (int bitNumber = 0; bitNumber < Long.SIZE; bitNumber++) {
                    int answIdx = bitNumber + (bufferNumber * Long.SIZE);
                    if (answer.get(answIdx)) {
                        answer.set(answIdx, (l & (1L << bitNumber)) > 0);
                    }
                }
            }
        }
        if (answer.cardinality() > 1) {
            throw new IllegalStateException();
        }
        int result = answer.nextSetBit(0);
        if (result == -1) {
            return Optional.empty();
        }
        return Optional.of(values.get(result));
    }

    @Override
    public I put(HasherCollection hashers) {
        Optional<I> result = get(hashers);
        if (!result.isPresent()) {
            int idx = busy.nextUnsetBit(0);
            if (idx < 0) {
                // extend the busy
                idx = busy.size();
                busy.resize(idx + 64);
                // int longCount = Double.valueOf(Math.ceil(shape.getNumberOfBits() / (double)
                // Long.SIZE)).intValue();
                buffer.add(new long[shape.getNumberOfBits()]);
            }
            BloomFilter filter = hashers.filterFor(shape);
            setBloomAt(idx, filter);
            busy.set(idx);
            while (values.size() < idx + 1) {
                values.add(null);
            }
            result = Optional.of(func.apply(filter));
            values.set(idx, result.get());
        }
        return result.get();
    }

    @Override
    public void remove(I index) {
        int idx = values.indexOf(index);
        if (idx > -1) {
            busy.unset(idx);
            clearBloomAt(idx);
            values.set(idx, null);
        }
    }

    @Override
    public Set<I> search(HasherCollection hashers) {
        EWAHCompressedBitmap answers = new EWAHCompressedBitmap();
        BloomFilter filter = hashers.filterFor(shape);

        class SearchFN implements IntPredicate {
            long w = ~0l;
            long[] buffer;

            SearchFN(long[] buffer) {
                this.buffer = buffer;
            }

            @Override
            public boolean test(int idx) {
                w &= buffer[idx];
                return true;
            }
        }
        ;

        for (int i = 0; i < buffer.size(); ++i) {
            SearchFN fn = new SearchFN(buffer.get(i));

            filter.forEachIndex(fn);

            while (fn.w != 0) {
                long t = fn.w & -fn.w;
                answers.set(i * Long.SIZE + Long.bitCount(t - 1));
                fn.w ^= t;
            }

        }
        IntIterator iter = answers.intIterator();
        Iterator<I> iterI = new Iterator<I>() {

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public I next() {
                return values.get(iter.next());
            }
        };
        // Convert the iterator to Spliterator
        Spliterator<I> spliterator = Spliterators.spliteratorUnknownSize(iterI, 0);
        return StreamSupport.stream(spliterator, false).collect(Collectors.toSet());
    }

    @Override
    public int getFilterCount() {
        return values.size();
    }

    @Override
    public Shape getShape() {
        return shape;
    }

    @Override
    public Set<I> getAll() {
        return new HashSet<I>(values);
    }

}
