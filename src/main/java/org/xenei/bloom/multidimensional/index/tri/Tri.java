package org.xenei.bloom.multidimensional.index.tri;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.bloomfilter.BitSetBloomFilter;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Hasher;
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
    private final long mask;

    protected Tri(Shape shape, int width, long mask) {
        this.shape = shape;
        this.width = width;
        this.mask = mask;
        this.list = new ArrayList<LeafNode>();
        this.empty = new BitSet(0);
        root = new InnerNode(0, shape, this, null);
    }

    protected abstract int[] getNodeIndexes(BloomFilter filter, int level);

    public final int getWidth() {
        return width;
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
    public final Stream<Integer> search(Hasher hasher) {
        BloomFilter filter = new BitSetBloomFilter(hasher, shape);
        return root.search(Container.emptyStream(), filter);
    }

    @Override
    public int get(Hasher hasher) {
        BloomFilter filter = new BitSetBloomFilter(hasher, shape);
        long[] filterLongs = filter.getBits();
        List<LeafNode> candidates = search(hasher).map(list::get).filter(l -> l != null).collect(Collectors.toList());
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

    private long[] assembleLongs(List<InnerNode> nodes, LeafNode leaf) {
        int limit = Double.valueOf(Math.ceil(shape.getNumberOfBits() / (double) Long.SIZE)).intValue();
        long[] result = new long[limit];

        for (int level = 0; level < nodes.size(); level++) {
            int longIdx = level * width / Long.SIZE;
            long val;
            if (level < nodes.size() - 1) {
                val = nodes.get(level).find(nodes.get(level + 1));
            } else {
                val = nodes.get(level).find(leaf);
            }
            if (val != 0) {
                result[longIdx] |= val << width * level;
            }
        }

        return result;
    }

    public final int getChunk(BloomFilter filter, int level) {
        long[] buffer = filter.getBits();

        int idx = level / Long.BYTES;
        if (idx >= buffer.length) {
            return 0x0;
        }
        int ofs = Math.floorMod(level * width, Long.SIZE);

        return (int) ((buffer[idx] >> ofs) & mask);
    }
}
