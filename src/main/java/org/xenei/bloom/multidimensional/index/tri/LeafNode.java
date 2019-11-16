package org.xenei.bloom.multidimensional.index.tri;

import org.apache.commons.collections4.bloomfilter.BloomFilter;

/**
 * A leaf node in the Trie.
 * A leaf node contains a single Bloom filter index.
 *
 */
public class LeafNode implements Node {
    /**
     * The index of the bloom filter in the Trie list.
     */
    private final int idx;
    /**
     * The inner node that points to this leaf.
     */
    private final InnerNode parent;

    /**
     * Constructs a leaf node.
     * @param idx The index of the Bloom filter.
     * @param parent the InnerNode that points to this leaf.
     */
    public LeafNode(int idx, InnerNode parent) {
        this.idx = idx;
        this.parent = parent;
    }

    /**
     * Gets the Bloom filter index.
     * @return the Bloom filter index
     */
    public int getIdx() {
        return idx;
    }

    @Override
    public String toString() {
        return String.format("LeafNode %s", idx);
    }

    @Override
    public LeafNode add(IndexedBloomFilter filter) {
        return this;
    }

    @Override
    public boolean remove(BloomFilter filter) {
        return true;
    }

    /**
     * Deletes this leaf node from the Trie.
     * The deletion will cascade up and remove any unneeded inner nodes in the process.
     */
    public void delete() {
        parent.remove(this);
    }

    @Override
    public InnerNode getParent() {
        return parent;
    }

}
