package org.xenei.bloom.multidimensional.index.tri;

import org.apache.commons.collections4.bloomfilter.BloomFilter;

/**
 * The shared definition of a Node in the Trie.*
 */
public interface Node {

    /**
     * Adds an IndexedBloomFilter to this node.
     * By definition this method will determine which chunk the filter belongs in on inner nodes
     * and then add it to the inner node at that chunk.  The inner node above the leaf node will return
     * the leaf node to the calling method and the stack will unwind.
     * @param filter the filter to add
     * @return the LeafNode where the filter was added.
     */
    public LeafNode add(IndexedBloomFilter filter);

    /**
     * Removes a Bloom filter from the index.
     * @param filter the filter to remove.
     * @return true if the node is empty after the removal.
     */
    public boolean remove(BloomFilter filter);

    /**
     * Gets the parent node of this node.
     * @return the parent node or {@code null} if this is the root node.
     */
    public InnerNode getParent();

}
