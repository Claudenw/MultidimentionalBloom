package org.xenei.bloom.filter;

import java.nio.charset.StandardCharsets;

import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.commons.collections4.bloomfilter.EnhancedDoubleHasher;
import org.apache.commons.collections4.bloomfilter.Hasher;

public class HasherFactory {
    private HasherFactory() {
    }

    public static Hasher hasherFor(String s) {
        long[] longs = MurmurHash3.hash128(s.getBytes(StandardCharsets.UTF_8));
        return new EnhancedDoubleHasher(longs[0], longs[1]);
    }

    public static HasherCollection collectionFor(String[] parts) {
        HasherCollection hashers = new HasherCollection();
        for (String part : parts) {
            hashers.add(hasherFor(part));
        }
        return hashers;
    }

}
