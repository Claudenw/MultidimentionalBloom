package org.xenei.bloom.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.apache.commons.collections4.bloomfilter.hasher.DynamicHasher;
import org.apache.commons.collections4.bloomfilter.hasher.MD5;
import org.junit.Test;

public class CachingHasherTest {

    @Test
    public void testSameValue() {
        CachingHasher hasher1 = new CachingHasher.Factory().useFunction(MD5.NAME).with("Hello World").build();
        DynamicHasher hasher2 = new DynamicHasher.Factory().useFunction(MD5.NAME).with("Hello World").build();

        Shape shape = new Shape(MD5.NAME, 3, 1.0 / 10000);
        Iterator<Integer> iter1 = hasher1.getBits(shape);
        Iterator<Integer> iter2 = hasher2.getBits(shape);

        while (iter2.hasNext()) {
            assertTrue("Caching iterator missing value", iter1.hasNext());
            assertEquals(iter2.next(), iter1.next());
        }
        assertFalse("Caching iterator has too many values", iter1.hasNext());

    }

}
