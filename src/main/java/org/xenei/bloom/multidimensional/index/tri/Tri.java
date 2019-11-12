package org.xenei.bloom.multidimensional.index.tri;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.bloomfilter.BitSetBloomFilter;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.HasherBloomFilter;
import org.apache.commons.collections4.bloomfilter.hasher.StaticHasher;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.xenei.bloom.multidimensional.Container;
import org.xenei.bloom.multidimensional.Container.Index;

import com.googlecode.javaewah.datastructure.BitSet;

public abstract class Tri implements Index {

    protected final Shape shape;
    private final List<LeafNode> list;
    private final BitSet empty;
    private InnerNode root;
    private final int width;

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

    protected Tri(Shape shape, int width) {
        this.shape = shape;
        this.width = width;
        this.list = new ArrayList<LeafNode>();
        this.empty = new BitSet(0);
        root = new InnerNode(0, shape, this, null);
    }

    public final int getWidth() {
        return width;
    }
    public abstract byte getChunk(BloomFilter filter, int level);
    public abstract int[] getNodeIndexes(BloomFilter filter, int level);

    @Override
    public final int put(Hasher hasher) {
        int idx = empty.nextSetBit(-1);
        if (idx == -1) {
            idx = list.size();
            list.add( null );
        } else {
            empty.unset(idx);
            if (empty.cardinality() == 0) {
                empty.resize(0);
            }
        }

        BloomFilter filter = new BitSetBloomFilter(hasher, shape);
        IndexedBloomFilter idxFilter = new IndexedBloomFilter(filter, idx);
        list.set( idx, root.add(idxFilter));
        return idx;
    }

    @Override
    public final void remove(int index) {
        LeafNode leaf = list.get(index);
        if (leaf != null)
        {
            leaf.delete();
            if (empty.length() < index) {
                empty.resize(index);
            }
            empty.set(index);
        }
    }

    @Override
    public final Stream<Integer> search(Hasher hasher) {
        BloomFilter filter = new BitSetBloomFilter(hasher, shape);
        return root.search(Container.emptyStream(), filter);
    }

    @Override
    public int get(Hasher hasher) {
        BloomFilter filter = new BitSetBloomFilter(hasher, shape);
        List<LeafNode> result = search(hasher).map( list::get ).filter( l -> l != null)
                .collect( Collectors.toList() );

        for (LeafNode leaf : result)
        {
            List<InnerNode> lst = new ArrayList<InnerNode>();
            Node n = leaf;
            while( n.getParent() != null)
            {
                lst.add( n.getParent() );
                n = n.getParent();
            }
            Collections.reverse( lst );
            BloomFilter bf = assembleFilter( lst, leaf );
            if (comp.compare(bf, filter) != 0)
            {
                result.remove(leaf);
            }
        }
        if (result.size() == 0 )
        {
            return -1;
        }
        else if (result.size() > 1)
        {
            throw new IllegalStateException( "Too many results: "+result.size() );
        }

        return result.get(0).getIdx();

    }

    private BloomFilter assembleFilter( List<InnerNode> nodes, LeafNode leaf )
    {
        List<Integer> lst = new ArrayList<Integer>();
        for (int level=0;level<nodes.size();level++)
        {

            int val;
            if (level < nodes.size() -1 )
            {
                val = nodes.get(level).find( nodes.get( level+1 ));
            }
            else
            {
                val = nodes.get(level).find( leaf );
            }
            for (int i=0;i<width;i++)
            {
                if ((val & (1<<i)) > 0)
                {
                    lst.add(1<< (i+(width*level)));
                }
            }
        }
        StaticHasher hasher = new StaticHasher( lst.iterator(), shape );
        return new HasherBloomFilter( hasher, shape );
    }

}
