package org.xenei.bloom.multidimensional.index.tri;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;

public class InnerNode implements Node {

    private final Tri tri;
    private final Node[] nodes;
    private final int level;
    private final int maxDepth;
    private final Shape shape;
    private final InnerNode parent;

    public InnerNode(int level, Shape shape, Tri tri, InnerNode parent) {
        this.tri = tri;
        this.shape = shape;
        this.level = level;
        this.parent = parent;
        this.maxDepth = (int) Math.ceil(shape.getNumberOfBits() * 1.0 / tri.getWidth());
        this.nodes = new Node[1 << tri.getWidth()];
    }

    public boolean isBaseNode() {
        return level + 1 == maxDepth;
    }

    public Node[] getLeafNodes() {
        return nodes;
    }

    @Override
    public LeafNode add(IndexedBloomFilter filter) {
        int chunk = tri.getChunk(filter.getFilter(), level);
        if (nodes[chunk] == null) {
            if ((level + 1) == maxDepth) {
                nodes[chunk] = new LeafNode(filter.getIdx(), this);
            } else {
                nodes[chunk] = new InnerNode(level + 1, shape, tri, this);
            }
        }
        return nodes[chunk].add(filter);
    }

    @Override
    public boolean remove(BloomFilter filter) {
        int chunk = tri.getChunk(filter, level);
        if (nodes[chunk] != null) {
            if (nodes[chunk].remove(filter)) {
                nodes[chunk] = null;
            }
            int buckets = 1 << tri.getWidth();
            for (int i = 0; i < buckets; i++) {
                if (nodes[i] != null) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public Stream<Integer> search(Stream<Integer> previous, BloomFilter filter) {
        int[] nodeIdxs = tri.getNodeIndexes(filter, level);
        Stream<Integer> result = previous;
        if (isBaseNode()) {
            List<Integer> newVals = new ArrayList<Integer>();
            for (int i : nodeIdxs) {
                if (nodes[i] != null) {
                    newVals.add(((LeafNode) nodes[i]).getIdx());
                }
            }
            result = Stream.concat(previous, newVals.stream());
        } else {
            for (int i : nodeIdxs) {
                if (nodes[i] != null) {
                    result = ((InnerNode) nodes[i]).search(result, filter);
                }
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return String.format("InnerNode d:%s", level);
    }

    public void remove(Node node) {
        boolean isEmpty = true;
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == node) {
                nodes[i] = null;
            } else {
                isEmpty &= nodes[i] == null;
            }
        }
        if (isEmpty) {
            if (parent != null) {
                parent.remove(this);
            }
        }
    }

    @Override
    public InnerNode getParent() {
        return parent;
    }

    public int find(Node node) {
        for (int i = 0; i < nodes.length; i++) {
            if (node.equals(nodes[i])) {
                return i;
            }
        }
        throw new IllegalArgumentException("Node was not found");
    }
}
