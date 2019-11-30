/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.bloom.multidimensional.index.tri;

import java.util.Set;
import org.apache.commons.collections4.bloomfilter.BloomFilter;

/**
 * An inner Trie node.
 *
 */
public class InnerNode<I> implements Node<I> {
    /**
     * The Trie this node belongs to.
     */
    private final Trie<I> trie;
    /**
     * The array of child nodes from this node.
     * The number of children is determined by the chunk size.
     */
    private final Node<I>[] nodes;
    /**
     * The level (aka: depth) at which this nodes sits in the trie.
     * zero based counting.
     */
    private final int level;

    /**
     * The parent node.
     */
    private final InnerNode<I> parent;

    /**
     * Constrcuts an innter node.
     * @param level the level at which this node sits.
     * @param trie the Trie in which this node sits.
     * @param parent the parent Node of this one.
     */
    @SuppressWarnings("unchecked")
    public InnerNode(int level, Trie<I> trie, InnerNode<I> parent) {
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
    public Node<I>[] getChildNodes() {
        return nodes;
    }

    @Override
    public LeafNode<I> add(BloomFilter filter) {
        int chunk = trie.getChunk(filter, level);
        if (nodes[chunk] == null) {
            if ((level + 1) == trie.getMaxDepth()) {
                nodes[chunk] = new LeafNode<I>(trie.makeIdx(filter), this);
            } else {
                nodes[chunk] = new InnerNode<I>(level + 1, trie, this);
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
    public void search(Set<I> indexes, BloomFilter filter) {
        int[] nodeIdxs = trie.getNodeIndexes(trie.getChunk(filter, level));
        if (isBaseNode()) {
            for (int i : nodeIdxs) {
                if (nodes[i] != null) {
                    indexes.add(((LeafNode<I>) nodes[i]).getIdx());
                }
            }
        } else {
            for (int i : nodeIdxs) {
                if (nodes[i] != null) {
                    ((InnerNode<I>) nodes[i]).search(indexes, filter);
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
    public void remove(Node<I> childNode) {
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
    public InnerNode<I> getParent() {
        return parent;
    }

    /**
     * Locates the child node within the list of child nodes.
     * @param childNode the child node to locate.
     * @return the index of the child node in the list.
     * @throws IllegalArgumentException if the child is not in the array of children.
     */
    public int find(Node<I> childNode) {
        for (int i = 0; i < nodes.length; i++) {
            if (childNode.equals(nodes[i])) {
                return i;
            }
        }
        throw new IllegalArgumentException("Node was not found");
    }
}
