package org.xenei.bloom.multidimensional.index.tri;

import org.apache.commons.collections4.bloomfilter.BloomFilter;

/**
 * A bloom filter and its associated index within the list of bloom filters in the Trie.
 * This class is used to insert filters in the tree.
 *
 */
public class IndexedBloomFilter {
    /**
     * The Bloom filter.
     */
    private BloomFilter filter;
    /**
     * The index of the Bloom filter in the Trie list.
     */
    private int idx;

    /**
     * Constructs an IndexedBloomFilter.
     * @param filter the Bloom filter.
     * @param idx the index of the Bloom filter.
     */
    public IndexedBloomFilter(BloomFilter filter, int idx) {
        this.filter = filter;
        this.idx = idx;
    }

    /**
     * Gets the Bloom filter.
     * @return the Bloom filter.
     */
    public BloomFilter getFilter() {
        return filter;
    }

    /**
     * Gets the index.
     * @return the index.
     */
    public int getIdx() {
        return idx;
    }

}
