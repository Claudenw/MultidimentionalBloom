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
package org.xenei.bloom.multidimensional;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.Hasher.Factory;
import org.apache.commons.collections4.bloomfilter.hasher.Murmur128;
import org.junit.Test;
import org.xenei.bloom.multidimensional.Container.Index;
import org.xenei.bloom.multidimensional.Container.Storage;
import org.xenei.bloom.multidimensional.index.FlatBloofi;
import org.xenei.bloom.multidimensional.storage.InMemory;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;

public class ContainerImplTest {
    Func func = new Func();
    Shape shape = new Shape(Murmur128.NAME, 3, 1.0 / 3000000);
    Storage<String,UUID> storage = new InMemory<String,UUID>();
    Index<UUID> index = new FlatBloofi<UUID>(func,shape);
    Container<String> container = new ContainerImpl<String,UUID>(shape, storage, index);

    @Test
    public void roundTrip() {
        String test = "Hello World";
        Hasher hasher = Factory.DEFAULT.useFunction(Murmur128.NAME).with(test).build();
        container.put(hasher, test);
        List<String> lst = new ArrayList<String>();
        container.get(hasher).forEachRemaining(lst::add);
        assertEquals(1, lst.size());
        assertEquals(test, lst.get(0));
    }

    private Hasher add(String s) {
        Hasher hasher = Factory.DEFAULT.useFunction(Murmur128.NAME).with(s).build();
        container.put(hasher, s);
        return hasher;
    }

    @Test
    public void getTest() {
        String test = "Hello World";
        Hasher hasher = add(test);
        add("Goodbye Cruel World");
        add("Now is the time for all good men to come to the aid of their country");

        List<String> lst = new ArrayList<String>();
        container.get(hasher).forEachRemaining(lst::add);
        assertEquals(1, lst.size());
        assertEquals(test, lst.get(0));
    }

    private Hasher makeHasher(String s) {
        String[] parts = s.split(" ");
        Hasher.Builder builder = Factory.DEFAULT.useFunction(Murmur128.NAME);
        for (String part : parts) {
            builder.with(part);
        }
        return builder.build();

    }

    @Test
    public void searchTest() {

        String test = "Hello World";
        String test2 = "Spring has Sprung";
        String test3 = "GoodBye Cruel World";
        container.put(makeHasher(test), test);
        container.put(makeHasher(test2), test2);
        container.put(makeHasher(test3), test3);

        Hasher hasher = makeHasher("Just another dog");
        List<String> lst = new ArrayList<String>();
        container.get(hasher).forEachRemaining(lst::add);
        assertEquals(0, lst.size());

        hasher = makeHasher("World");
        container.search(hasher).forEachRemaining(lst::add);
        assertEquals(2, lst.size());
        assertEquals(test, lst.get(0));
        assertEquals(test3, lst.get(1));
    }

    @Test
    public void removeTest() {
        String test = "Hello World";
        Hasher hasher = add(test);
        add("Goodbye Cruel World");
        add("Now is the time for all good men to come to the aid of their country");

        List<String> lst = new ArrayList<String>();
        container.get(hasher).forEachRemaining(lst::add);
        assertEquals(1, lst.size());
        assertEquals(test, lst.get(0));

        container.remove(hasher, "Hello World too");
        lst = new ArrayList<String>();
        container.get(hasher).forEachRemaining(lst::add);
        assertEquals(1, lst.size());
        assertEquals(test, lst.get(0));

        container.remove(hasher, "Hello World");
        lst = new ArrayList<String>();
        container.get(hasher).forEachRemaining(lst::add);
        assertEquals(0, lst.size());

    }

    /**
     * A standard Func to use in testing where UUID creation is desired.
     *
     */
    public static class Func implements Function<BloomFilter,UUID> {

        private byte[] getBytes( BloomFilter filter)
        {
            byte[] buffer = new byte[filter.getShape().getNumberOfBytes()];
            long[] lBuffer = filter.getBits();
            for (int i=0;i<buffer.length;i++)
            {
                int longIdx = i / Long.BYTES;
                int longOfs = i % Long.BYTES;
                if (longIdx >= lBuffer.length)
                {
                    return buffer;
                }
                buffer[i] = (byte) ((lBuffer[longIdx]>>(Byte.SIZE * longOfs))  & 0xFFL);
            }
            return buffer;
        }

        @Override
        public UUID apply(BloomFilter filter) {
            return UUID.nameUUIDFromBytes(getBytes( filter ));
        }

    }
}
