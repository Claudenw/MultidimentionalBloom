package org.xenei.bloom.multidimensional.index.tri;

import org.apache.commons.collections4.bloomfilter.BloomFilter;

public class LeafNode implements Node {
    private final int idx;
    private final InnerNode parent;

    public LeafNode(int idx, InnerNode parent) {
        this.idx = idx;
        this.parent = parent;
    }

    public int getIdx() {
        return idx;
    }

    @Override
    public String toString()
    {
        return String.format( "LeafNode %s", idx );
    }

    @Override
    public LeafNode add(IndexedBloomFilter filter) {
        return this;
    }

    @Override
    public boolean remove(BloomFilter filter) {
        return true;
    }

    public void delete() {
        parent.remove( this );

    }

    @Override
    public InnerNode getParent() {
        return parent;
    }

}
