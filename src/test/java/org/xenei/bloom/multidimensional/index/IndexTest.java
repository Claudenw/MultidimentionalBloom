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
import static org.junit.Assert.assertTrue;

import java.util.Set;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.junit.After;
import org.junit.Before;
import org.xenei.bloom.filter.CachingHasher;
import org.xenei.bloom.multidimensional.Container;
import org.xenei.bloom.multidimensional.Container.Index;
import org.xenei.junit.contract.Contract;
import org.xenei.junit.contract.ContractTest;
import org.xenei.junit.contract.IProducer;
import org.xenei.junit.contract.Contract.Inject;

@Contract(Container.Index.class)
public class IndexTest {
    private final static String HASH_NAME = "Test-IC";
    public final static Shape SHAPE = new Shape(HASH_NAME, 3, 1.0 / 10000);
    IProducer<Index> producer;
    Index index;

    @Inject
    public void setProducer(IProducer<Index> producer) {
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
        CachingHasher hasher1 = new CachingHasher(HASH_NAME, new long[][] { { 29, 0 }, { 13, 0 } });
        int idx = index.put(hasher1);
        assertEquals(idx, index.get(hasher1));
    }

    @ContractTest
    public void getTest_NotFound() {
        CachingHasher hasher1 = new CachingHasher(HASH_NAME, new long[][] { { 29, 0 }, { 13, 0 } });
        assertEquals(Index.NOT_FOUND, index.get(hasher1));
    }

    @ContractTest
    public void getTest_PartialMatch() {
        CachingHasher hasher1 = new CachingHasher(HASH_NAME, new long[][] { { 29, 0 }, { 13, 0 } });
        CachingHasher hasher2 = new CachingHasher(HASH_NAME, new long[][] { { 29, 0 } });
        index.put(hasher1);
        assertEquals(Index.NOT_FOUND, index.get(hasher2));
    }

    @ContractTest
    public void removeTest() {
        CachingHasher hasher1 = new CachingHasher(HASH_NAME, new long[][] { { 29, 0 }, { 13, 0 } });
        int idx = index.put(hasher1);
        assertEquals(idx, index.get(hasher1));
        index.remove(idx);
        assertEquals(Index.NOT_FOUND, index.get(hasher1));
    }

    @ContractTest
    public void searchTest() {
        CachingHasher hasher1 = new CachingHasher(HASH_NAME, new long[][] { { 29, 0 }, { 13, 0 } });
        CachingHasher hasher2 = new CachingHasher(HASH_NAME, new long[][] { { 29, 0 }, { 14, 0 } });
        CachingHasher hasher3 = new CachingHasher(HASH_NAME, new long[][] { { 30, 0 }, { 13, 0 } });
        CachingHasher hasher4 = new CachingHasher(HASH_NAME, new long[][] { { 29, 0 }, { 30, 0 } });
        int idx1 = index.put(hasher1);
        int idx2 = index.put(hasher2);
        int idx3 = index.put(hasher3);
        int idx4 = index.put(hasher4);

        CachingHasher search = new CachingHasher(HASH_NAME, new long[][] { { 29, 0 }, { 13, 0 } });
        Set<Integer> result = index.search(search);
        assertEquals(1, result.size());
        assertEquals(idx1, result.iterator().next().intValue());

        search = new CachingHasher(HASH_NAME, new long[][] { { 29, 0 } });
        result = index.search(search);
        assertEquals(3, result.size());
        assertTrue(result.contains(Integer.valueOf(idx1)));
        assertTrue(result.contains(Integer.valueOf(idx2)));
        assertTrue(result.contains(Integer.valueOf(idx4)));

        search = new CachingHasher(HASH_NAME, new long[][] { { 13, 0 } });
        result = index.search(search);
        assertEquals(2, result.size());
        assertTrue(result.contains(Integer.valueOf(idx1)));
        assertTrue(result.contains(Integer.valueOf(idx3)));

        search = new CachingHasher(HASH_NAME, new long[][] { { 29, 0 }, { 13, 0 }, { 14, 0 } });
        result = index.search(search);
        assertEquals(0, result.size());

    }

}
