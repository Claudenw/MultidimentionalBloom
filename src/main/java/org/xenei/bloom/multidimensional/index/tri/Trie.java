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
package org.xenei.bloom.multidimensional.index.tri;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.bloomfilter.BitSetBloomFilter;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.xenei.bloom.multidimensional.Container.Index;

import com.googlecode.javaewah.datastructure.BitSet;

/**
 * An asbtract Trie implementation.
 *
 * Bloom filters are chunked and each chunk is used to determine a leaf node where the filter is stored.
 * When searching each chunk is expanded into the matching chunks as per the Bloom filter matching
 * algorithm and all branches explored.
 *
 */
public abstract class Trie implements Index {

    /**
     * The shape of the contained Bloom filters.
     */
    protected final Shape shape;
    /**
     * A list of all leaf nodes created in the system
     */
    private final List<LeafNode> list;
    /**
     * A bitset that indicates the leaf nodes in the list that have been deleted (set to null).
     * This is used for deletion and to reuse list entries.
     */
    private final BitSet empty;
    /**
     * The root node of the Trie.
     */
    private InnerNode root;
    /**
     * The size of the chunks in the Trie in bits.
     */
    private final int chunkSize;
    /**
     * The mask for a single chunk.
     */
    private final long mask;

    /**
     * Constructs a Trie.
     * @param estimatedPopulation the estimated number of Bloom filters to be indexed.
     * @param shape the shape of the contained Bloom filters.
     * @param chunkSize the size of the Trie chunks in bits.
     * @param mask the mask to extract a single chunk.
     */
    protected Trie(int estimatedPopulation, Shape shape, int chunkSize, long mask) {
        this.shape = shape;
        this.chunkSize = chunkSize;
        this.mask = mask;
        this.list = new ArrayList<LeafNode>(estimatedPopulation);
        this.empty = new BitSet(0);
        root = new InnerNode(0, this, null);
    }
    /**
     * Returns the matching nodes for a specific level in the Trie.
     * The concrete class will translate the chunk into all possible matching chunks.
     * The returned values must be unique but need not be ordered.
     * @param filter the filter to extract the matching nodes from.
     * @param level the level of the Trie that we are checking.
     * @return an array of all matching chunks.
     */
    protected abstract int[] getNodeIndexes(int chunk);

    /**
     * Get the chunk size for this Trie.
     * @return the chunk size for this trie.
     */
    public final int getChunkSize() {
        return chunkSize;
    }

    @Override
    public final int put(Hasher hasher) {
        int idx = empty.nextSetBit(-1);
        if (idx == -1) {
            idx = list.size();
            list.add(null);
        } else {
            empty.unset(idx);
            if (empty.cardinality() == 0) {
                empty.resize(0);
            }
        }

        BloomFilter filter = new BitSetBloomFilter(hasher, shape);
        IndexedBloomFilter idxFilter = new IndexedBloomFilter(filter, idx);
        list.set(idx, root.add(idxFilter));
        return idx;
    }

    @Override
    public final void remove(int index) {
        LeafNode leaf = list.get(index);
        if (leaf != null) {
            leaf.delete();
            if (empty.size() <= index) {
                empty.resize(index + 1);
            }
            empty.set(index);
        }
    }

    @Override
    public final Set<Integer> search(Hasher hasher) {
        BloomFilter filter = new BitSetBloomFilter(hasher, shape);
        Set<Integer> result = new HashSet<Integer>();
        root.search(result, filter);
        return result;
    }

    @Override
    public int get(Hasher hasher) {
        BloomFilter filter = new BitSetBloomFilter(hasher, shape);
        long[] filterLongs = filter.getBits();
        List<LeafNode> candidates = search(hasher).stream().map(list::get).filter(l -> l != null).collect(Collectors.toList());
        int result = Index.NOT_FOUND;
        for (LeafNode leaf : candidates) {
            List<InnerNode> lst = new ArrayList<InnerNode>();
            Node n = leaf;
            while (n.getParent() != null) {
                lst.add(n.getParent());
                n = n.getParent();
            }
            Collections.reverse(lst);
            long[] values = assembleLongs(lst, leaf);
            if (Arrays.equals(values, filterLongs)) {
                if (result != Index.NOT_FOUND) {
                    throw new IllegalStateException("Too many results");
                }
                result = leaf.getIdx();
            }
        }

        return result;

    }

    /**
     * Get the array of long representation of the Bloom filter specified by the leaf node.
     * The Bloom filter is encoded into the Trie, this method extracts it.
     * @param nodes the inner nodes that point to the leaf node in order from root to inner node above leaf.
     * @param leaf the leaf node.
     * @return the long[] representation of the Bloom filter stored on the leaf.
     */
    private long[] assembleLongs(List<InnerNode> nodes, LeafNode leaf) {
        int limit = Double.valueOf(Math.ceil(shape.getNumberOfBits() / (double) Long.SIZE)).intValue();
        long[] result = new long[limit];

        for (int level = 0; level < nodes.size(); level++) {
            int longIdx = level * chunkSize / Long.SIZE;
            long val;
            if (level < nodes.size() - 1) {
                val = nodes.get(level).find(nodes.get(level + 1));
            } else {
                val = nodes.get(level).find(leaf);
            }
            if (val != 0) {
                result[longIdx] |= val << chunkSize * level;
            }
        }

        return result;
    }

    /**
     * Get the chunk for a specific level from a Bloom filter.
     * @param filter the Bloom filter to extract the chunk from.
     * @param level the level of the chunk.
     * @return the specified chunk.
     */
    public final int getChunk(BloomFilter filter, int level) {
        long[] buffer = filter.getBits();

        int idx = level / Long.BYTES;
        if (idx >= buffer.length) {
            return 0x0;
        }
        int ofs = Math.floorMod(level * chunkSize, Long.SIZE);

        return (int) ((buffer[idx] >> ofs) & mask);
    }

    /**
     * Gets the shape of the Bloom filters in this Trie.
     * @return the shape of the contained Bloom filters.
     */
    protected Shape getShape() {
        return shape;
    }

    public int getMaxDepth() {
       return (int) Math.ceil(shape.getNumberOfBits() * 1.0 / getChunkSize());
    }
}
