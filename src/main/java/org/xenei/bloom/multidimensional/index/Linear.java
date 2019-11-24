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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.xenei.bloom.filter.EWAHBloomFilter;
import org.xenei.bloom.multidimensional.Container.Index;

import com.googlecode.javaewah.datastructure.BitSet;

/**
 * A linear implementation of an index.
 * <p>
 * This implementation is adequate for smallish populations.  Large populations with
 * high collision rates will see significant performance degredation.
 * </p>
 */
public class Linear implements Index {
    /**
     * The shape of the bloom filters.
     */
    private Shape shape;
    /**
     * The list of bloom filters.
     */
    private List<BloomFilter> list;
    /**
     * A bitset that tracks which entries in the list are empty (available for resuse).
     */
    private BitSet empty;

    /**
     * A comparator for bloom filters to determine if they are bit for bit identical.
     */
    private static Comparator<BloomFilter> comp = new Comparator<BloomFilter>() {
        /**
         * Comparator for two Filters. Compares them based on byte values. Primarily
         * used to compare filters for equality.
         *
         * @param filter1 the first filter.
         * @param filter2 the second filter.
         * @return -1, 0 or 1 as per Comparator
         * @see Comparator
         */
        @Override
        public int compare(BloomFilter filter1, BloomFilter filter2) {
            if (filter1 == null) {
                return filter2 == null ? 0 : -1;
            } else {
                if (filter2 == null) {
                    return 1;
                }
            }
            long[] num1 = filter1.getBits();
            long[] num2 = filter2.getBits();
            int limit = Integer.min(num1.length, num2.length);
            for (int i = 0; i < limit; i++) {
                int result = Long.compare(num1[i], num2[i]);
                if (result != 0) {
                    return result;
                }
            }
            return Integer.compare(num1.length, num2.length);
        }
    };

    /**
     * Constructs the Linear index.
     * Uses 1/shape.getProbability() as the estimated number of filters.
     * @param shape the shape of the bloom filters.
     */
    public Linear(Shape shape) {
        this( Double.valueOf( 1.0/shape.getProbability() ).intValue(), shape );
    }

    /**
     * Constructs the Linear index.
     * @param estimatedPopulation the estimated number of Bloom filters to index.
     * @param shape the shape of the bloom filters.
     */
    public Linear(int estimatedPopulation, Shape shape) {
        this.shape = shape;
        list = new ArrayList<BloomFilter>(estimatedPopulation);
        empty = new BitSet(0);
    }

    @Override
    public int get(Hasher hasher) {
        BloomFilter bf = new EWAHBloomFilter(hasher, shape);
        for (int i = 0; i < list.size(); i++) {
            if (comp.compare(list.get(i), bf) == 0) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public int put(Hasher hasher) {
        BloomFilter filter = new EWAHBloomFilter(hasher, shape);
        int idx = empty.nextSetBit(-1);
        if (idx == -1) {
            list.add(filter);
            idx = list.size() - 1;
        } else {
            list.set(idx, filter);
            empty.unset(idx);
            if (empty.cardinality() == 0) {
                empty.resize(0);
            }
        }
        return idx;
    }

    @Override
    public void remove(int index) {
        list.set(index, null);
        if (empty.size() <= index) {
            empty.resize(index + 1);
        }
        empty.set(index);
    }

    @Override
    public Set<Integer> search(Hasher hasher) {
        Set<Integer> result = new HashSet<Integer>();
        BloomFilter bf = new EWAHBloomFilter(hasher, shape);
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).contains(bf)) {
                result.add(i);
            }
        }
        return result;
    }

}
