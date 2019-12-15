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
package org.xenei.bloom.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;

import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.apache.commons.collections4.bloomfilter.hasher.DynamicHasher;
import org.apache.commons.collections4.bloomfilter.hasher.HashFunction;
import org.apache.commons.collections4.bloomfilter.hasher.function.MD5Cyclic;
import org.apache.commons.collections4.bloomfilter.hasher.function.ObjectsHashIterative;
import org.junit.Test;

public class CachingHasherTest {

    @Test
    public void testSameValue() {
        HashFunction hashFunction = new MD5Cyclic();
        CachingHasher hasher1 = new CachingHasher.Builder( hashFunction ).with("Hello World").build();
        DynamicHasher hasher2 = new DynamicHasher.Builder( hashFunction ).with("Hello World").build();

        Shape shape = new Shape(hashFunction, 3, 1.0 / 10000);
        Iterator<Integer> iter1 = hasher1.getBits(shape);
        Iterator<Integer> iter2 = hasher2.getBits(shape);

        while (iter2.hasNext()) {
            assertTrue("Caching iterator missing value", iter1.hasNext());
            assertEquals(iter2.next(), iter1.next());
        }
        assertFalse("Caching iterator has too many values", iter1.hasNext());

    }

    @Test
    public void testInvalidName() {

        try {
            new CachingHasher.Builder(new ObjectsHashIterative());
            fail( "Should have thrown IllegalArgumentException");
        }
        catch (IllegalArgumentException e)
        {
            // exected do nothing.
        }
        try {
            new CachingHasher(new ObjectsHashIterative(), new long[][] {{1L, 2L}});
            fail( "Should have thrown IllegalArgumentException");
        }
        catch (IllegalArgumentException e)
        {
            // exected do nothing.
        }

    }


}
