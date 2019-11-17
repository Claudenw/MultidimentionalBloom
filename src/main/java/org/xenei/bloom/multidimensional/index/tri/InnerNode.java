package org.xenei.bloom.multidimensional.index.tri;

import java.util.Set;
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
        this.nodes = new Node[1 << trie.getChunkSize()];
    }

    /**
     * Gets base node status.
     * A base node is an inner node just above the leaf nodes.
     * @return true if this is a  base node.
     */
    public boolean isBaseNode() {
        return level + 1 == trie.getMaxDepth();
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
            if ((level + 1) == trie.getMaxDepth()) {
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
     * Searches this inner node and populate the set of Bloom filter index values from the leaf
     * nodes.
     * @param indexes The set of Bloom filter indexes.
     * @param filter the filter we are looking for.
     */
    public void search(Set<Integer> indexes, BloomFilter filter) {
        int[] nodeIdxs = trie.getNodeIndexes(trie.getChunk(filter, level));
        if (isBaseNode()) {
            for (int i : nodeIdxs) {
                if (nodes[i] != null) {
                    indexes.add(((LeafNode) nodes[i]).getIdx());
                }
            }
        } else {
            for (int i : nodeIdxs) {
                if (nodes[i] != null) {
                    ((InnerNode) nodes[i]).search(indexes, filter);
                }
            }
        }
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
