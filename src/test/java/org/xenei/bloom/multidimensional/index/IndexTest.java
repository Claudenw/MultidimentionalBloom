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
package org.xenei.bloom.multidimensional.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.collections4.bloomfilter.EnhancedDoubleHasher;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.junit.After;
import org.junit.Before;
import org.xenei.bloom.filter.HasherCollection;
import org.xenei.bloom.multidimensional.Container;
import org.xenei.bloom.multidimensional.Container.Index;
import org.xenei.junit.contract.Contract;
import org.xenei.junit.contract.Contract.Inject;
import org.xenei.junit.contract.ContractTest;
import org.xenei.junit.contract.IProducer;

@Contract(Container.Index.class)
public class IndexTest {

    public final static int N = 10000;
    public final static Shape SHAPE = Shape.fromNP(3, 1.0 / N);
    IProducer<Index<UUID>> producer;
    Index<UUID> index;

    @Inject
    public void setProducer(IProducer<Index<UUID>> producer) {
        this.producer = producer;
    }

    @Before
    public void setup() {
        index = producer.newInstance();
    }

    @After
    public void cleanup() {
        producer.cleanUp();
    }

    @ContractTest
    public void getTest() {
        HasherCollection hashers = new HasherCollection();
        hashers.add(new EnhancedDoubleHasher(29, 3));
        UUID idx = index.put(hashers);
        hashers.clear();
        hashers.add(new EnhancedDoubleHasher(29, 3));
        Optional<UUID> opt = index.get(hashers);
        assertTrue(opt.isPresent());
        assertEquals(idx, opt.get());
    }

    @ContractTest
    public void getTest_NotFound() {
        HasherCollection hashers = new HasherCollection();
        hashers.add(new EnhancedDoubleHasher(29, 3));
        assertFalse(index.get(hashers).isPresent());
    }

    @ContractTest
    public void getTest_PartialMatch() {
        HasherCollection hashers = new HasherCollection();
        Hasher hasher1 = new EnhancedDoubleHasher(29, 3);
        hashers.add(hasher1);
        index.put(hashers);
        hashers.add(new EnhancedDoubleHasher(13, 3));
        assertFalse(index.get(hashers).isPresent());
    }

    @ContractTest
    public void removeTest() {
        HasherCollection hashers = new HasherCollection();
        Hasher hasher1 = new EnhancedDoubleHasher(29, 3);
        hashers.add(hasher1);
        UUID idx = index.put(hashers);
        Optional<UUID> opt = index.get(hashers);
        assertTrue(opt.isPresent());
        assertEquals(idx, opt.get());
        index.remove(idx);
        assertFalse(index.get(hashers).isPresent());
    }

    @ContractTest
    public void searchTest() {
        HasherCollection hashers = new HasherCollection();

        Hasher hasher1 = new EnhancedDoubleHasher(10, 1);
        hashers.add(hasher1);
        hashers.filterFor(index.getShape());
        UUID idx1 = index.put(hashers);
        hashers.clear();

        Hasher hasher2 = new EnhancedDoubleHasher(11, 1);
        hashers.add(hasher2);
        UUID idx2 = index.put(hashers);
        hashers.clear();

        Hasher hasher3 = new EnhancedDoubleHasher(12, 1);
        hashers.add(hasher3);
        UUID idx3 = index.put(hashers);
        hashers.clear();

        Hasher hasher4 = new EnhancedDoubleHasher(13, 1);
        hashers.add(hasher4);
        UUID idx4 = index.put(hashers);
        hashers.clear();

        hashers.add(new FixedHasher(0));
        Set<UUID> result = index.search(hashers);
        assertEquals(1, result.size());
        assertEquals(idx1, result.iterator().next());
        hashers.clear();

        hashers.add(new FixedHasher(10));
        result = index.search(hashers);
        assertEquals(3, result.size());
        assertTrue(result.contains(idx1));
        assertTrue(result.contains(idx2));
        assertTrue(result.contains(idx3));
        assertFalse(result.contains(idx4));
    }

}
