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

import org.apache.commons.collections4.bloomfilter.BloomFilter;

/**
 * The shared definition of a Node in the Trie.*
 */
public interface Node<I> {

    /**
     * Adds an IndexedBloomFilter to this node.
     * By definition this method will determine which chunk the filter belongs in on inner nodes
     * and then add it to the inner node at that chunk.  The inner node above the leaf node will return
     * the leaf node to the calling method and the stack will unwind.
     * @parma idx the index to use in the leaf.
     * @param filter the filter to add
     * @return the LeafNode where the filter was added.
     */
    public LeafNode<I> add(I idx, long[] bitMaps);

    /**
     * Removes a Bloom filter from the index.
     * @param filter the filter to remove.
     * @return true if the node is empty after the removal.
     */
    public boolean remove(long[] bitMaps);

    /**
     * Gets the parent node of this node.
     * @return the parent node or {@code null} if this is the root node.
     */
    public InnerNode<I> getParent();

}
