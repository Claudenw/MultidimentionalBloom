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

/**
 * A leaf node in the Trie. A leaf node contains a single Bloom filter index.
 *
 */
public class LeafNode<I> implements Node<I> {
    /**
     * The index of the bloom filter in the Trie list.
     */
    private final I idx;
    /**
     * The inner node that points to this leaf.
     */
    private final InnerNode<I> parent;

    /**
     * Constructs a leaf node.
     * 
     * @param idx The index of the Bloom filter.
     * @param parent the InnerNode that points to this leaf.
     */
    public LeafNode(I idx, InnerNode<I> parent) {
        this.idx = idx;
        this.parent = parent;
    }

    /**
     * Gets the Bloom filter index.
     * 
     * @return the Bloom filter index
     */
    public I getIdx() {
        return idx;
    }

    @Override
    public String toString() {
        return String.format("LeafNode %s", idx);
    }

    @Override
    public LeafNode<I> add(I idx, long[] bitMap) {
        return this;
    }

    @Override
    public boolean remove(long[] bitMap) {
        return true;
    }

    /**
     * Deletes this leaf node from the Trie. The deletion will cascade up and remove
     * any unneeded inner nodes in the process.
     */
    public void delete() {
        parent.remove(this);
    }

    @Override
    public InnerNode<I> getParent() {
        return parent;
    }

}
