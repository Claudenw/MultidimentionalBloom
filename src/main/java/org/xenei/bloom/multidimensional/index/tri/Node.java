package org.xenei.bloom.multidimensional.index.tri;

import org.apache.commons.collections4.bloomfilter.BloomFilter;



public interface Node {
    public LeafNode add(IndexedBloomFilter filter);
    /**
     * Return true if the node was removed.
     * @param filter
     * @return
     */
    public boolean remove(BloomFilter filter);

    public InnerNode getParent();


}
