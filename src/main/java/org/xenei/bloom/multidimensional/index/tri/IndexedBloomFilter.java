package org.xenei.bloom.multidimensional.index.tri;

import org.apache.commons.collections4.bloomfilter.BloomFilter;

public class IndexedBloomFilter  {
    private BloomFilter filter;
    private int idx;

    public IndexedBloomFilter( BloomFilter filter, int idx )
    {
        this.filter = filter;
        this.idx = idx;
    }

    public BloomFilter getFilter() {
        return filter;
    }

    public int getIdx() {
        return idx;
    }

}
