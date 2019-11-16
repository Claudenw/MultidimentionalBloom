package org.xenei.bloom.multidimensional.index.tri;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.collections4.bloomfilter.BloomFilter;

/**
 * An inner Trie node.
 *
 */
public class InnerNode implements Node {
    /**
     * The Trie this node belongs to.
     */
    private final Trie trie;
    /**
     * The array of child nodes from this node.
     * The number of children is determined by the chunk size.
     */
    private final Node[] nodes;
    /**
     * The level (aka: depth) at which this nodes sits in the trie.
     * zero based counting.
     */
    private final int level;
    /**
     * The maximum depth of the trie.  zero based.
     */
    private final int maxDepth;

    /**
     * The parent node.
     */
    private final InnerNode parent;

    /**
     * Constrcuts an innter node.
     * @param level the level at which this node sits.
     * @param trie the Trie in which this node sits.
     * @param parent the parent Node of this one.
     */
    public InnerNode(int level, Trie trie, InnerNode parent) {
        this.trie = trie;
        this.level = level;
        this.parent = parent;
        this.maxDepth = (int) Math.ceil(trie.getShape().getNumberOfBits() * 1.0 / trie.getChunkSize());
        this.nodes = new Node[1 << trie.getChunkSize()];
    }

    /**
     * Gets base node status.
     * A base node is an inner node just above the leaf nodes.
     * @return true if this is a  base node.
     */
    public boolean isBaseNode() {
        return level + 1 == maxDepth;
    }

    /**
     * Get the nodes below this one.
     * The array may contain null values.
     * @return the array of nodes below this one.
     */
    public Node[] getChildNodes() {
        return nodes;
    }

    @Override
    public LeafNode add(IndexedBloomFilter filter) {
        int chunk = trie.getChunk(filter.getFilter(), level);
        if (nodes[chunk] == null) {
            if ((level + 1) == maxDepth) {
                nodes[chunk] = new LeafNode(filter.getIdx(), this);
            } else {
                nodes[chunk] = new InnerNode(level + 1, trie, this);
            }
        }
        return nodes[chunk].add(filter);
    }

    @Override
    public boolean remove(BloomFilter filter) {
        int chunk = trie.getChunk(filter, level);
        if (nodes[chunk] != null) {
            if (nodes[chunk].remove(filter)) {
                nodes[chunk] = null;
            }
            int buckets = 1 << trie.getChunkSize();
            for (int i = 0; i < buckets; i++) {
                if (nodes[i] != null) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Searches this inner node and return the stream of Bloom filter index values from the leaf
     * nodes.
     * This method must create a stream of matching indexes from the nodes below it and concatenate that
     * to the previous stream and then return the result.
     * @param previous The stream from the previously searched inner nodes.
     * @param filter the filter we are looking for.
     * @return the Stream of integers that are indexes to matching Bloom filters.
     */
    public Stream<Integer> search(Stream<Integer> previous, BloomFilter filter) {
        int[] nodeIdxs = trie.getNodeIndexes(trie.getChunk(filter, level));
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

    /**
     * Removes the child node from this node.
     * If this node becomes empty during this operation it must remove itself
     * from its parent as well.
     * @param childNode the node to remove from the array of children.
     */
    public void remove(Node childNode) {
        boolean isEmpty = true;
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == childNode) {
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

    /**
     * Locates the child node within the list of child nodes.
     * @param childNode the child node to locate.
     * @return the index of the child node in the list.
     * @throws IllegalArgumentException if the child is not in the array of children.
     */
    public int find(Node childNode) {
        for (int i = 0; i < nodes.length; i++) {
            if (childNode.equals(nodes[i])) {
                return i;
            }
        }
        throw new IllegalArgumentException("Node was not found");
    }
}
