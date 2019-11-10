package com.googlecode.javaewah.datastructure;

import java.util.Arrays;

/**
 * This is an optimized version of Java's BitSet.
 *
 * @author Daniel Lemire
 * @since 0.8.0
 **/
public final class BitSet implements Cloneable {
    /**
     * Construct a bitset with the specified number of bits (initially all false).
     * The number of bits is rounded up to the nearest multiple of 64.
     *
     * @param sizeinbits the size in bits
     */
    public BitSet(final int sizeinbits) {
        if (sizeinbits < 0)
            throw new NegativeArraySizeException("negative number of bits: " + sizeinbits);
        this.data = new long[(sizeinbits + 63) / 64];
    }

    public int andcardinality(BitSet bs) {
        if (data.length != bs.data.length)
            throw new IllegalArgumentException("incompatible bitsets");
        int sum = 0;
        for (int k = 0; k < data.length; ++k) {
            sum += Long.bitCount(data[k] & bs.data[k]);
        }
        return sum;
    }

    /**
     * Compute the number of bits set to 1
     *
     * @return the number of bits
     */
    public int cardinality() {
        int sum = 0;
        for (long l : this.data)
            sum += Long.bitCount(l);
        return sum;
    }

    /**
     * Reset all bits to false
     */
    public void clear() {
        Arrays.fill(this.data, 0);
    }

    @Override
    public BitSet clone() {
        BitSet b;
        try {
            b = (BitSet) super.clone();
            b.data = Arrays.copyOf(this.data, this.data.length);
            return b;
        } catch (CloneNotSupportedException e) {
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BitSet))
            return false;
        return Arrays.equals(data, ((BitSet) o).data);
    }

    /**
     * @param i index
     * @return value of the bit
     */
    public boolean get(final int i) {
        return (this.data[i / 64] & (1l << (i % 64))) != 0;
    }

    public long getWord(int i) {
        return data[i];
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    /**
     * Query the size
     *
     * @return the size in bits.
     */
    public int length() {
        return this.data.length * 64;
    }

    /**
     * Usage: for(int i=bs.nextSetBit(0); i&gt;=0; i=bs.nextSetBit(i+1)) { operate
     * on index i here }
     *
     * @param i current set bit
     * @return next set bit or -1
     */
    public int nextSetBit(final int i) {
        int x = i / 64;
        if (x >= this.data.length)
            return -1;
        long w = this.data[x];
        w >>>= (i % 64);
        if (w != 0) {
            return i + Long.numberOfTrailingZeros(w);
        }
        ++x;
        for (; x < this.data.length; ++x) {
            if (this.data[x] != 0) {
                return x * 64 + Long.numberOfTrailingZeros(this.data[x]);
            }
        }
        return -1;
    }

    /**
     * Usage: for(int i=bs.nextUnsetBit(0); i&gt;=0; i=bs.nextUnsetBit(i+1)) {
     * operate on index i here }
     *
     * @param i current unset bit
     * @return next unset bit or -1
     */
    public int nextUnsetBit(final int i) {
        int x = i / 64;
        if (x >= this.data.length)
            return -1;
        long w = ~this.data[x];
        w >>>= (i % 64);
        if (w != 0) {
            return i + Long.numberOfTrailingZeros(w);
        }
        ++x;
        for (; x < this.data.length; ++x) {
            if (this.data[x] != ~0) {
                return x * 64 + Long.numberOfTrailingZeros(~this.data[x]);
            }
        }
        return -1;
    }

    /**
     * Compute bitwise OR, assumes that both bitsets have the same length.
     *
     * @param bs other bitset
     */
    public void or(BitSet bs) {
        if (data.length != bs.data.length)
            throw new IllegalArgumentException("incompatible bitsets");
        for (int k = 0; k < data.length; ++k) {
            data[k] |= bs.data[k];
        }
    }

    public int orcardinality(BitSet bs) {
        if (data.length != bs.data.length)
            throw new IllegalArgumentException("incompatible bitsets");
        int sum = 0;
        for (int k = 0; k < data.length; ++k) {
            sum += Long.bitCount(data[k] | bs.data[k]);
        }
        return sum;
    }

    public void removeWord(int i) {
        long[] newdata = new long[data.length - 1];
        if (i == 0) {
            System.arraycopy(data, 1, newdata, 0, i - 1);
        }
        System.arraycopy(data, 0, newdata, 0, i - 1);
        System.arraycopy(data, i, newdata, i - 1, data.length - i);
        data = newdata;
    }

    /**
     * Resize the bitset
     *
     * @param sizeinbits new number of bits
     */
    public void resize(int sizeinbits) {
        this.data = Arrays.copyOf(this.data, (sizeinbits + 63) / 64);
    }

    /**
     * Set to true
     *
     * @param i index of the bit
     */
    public void set(final int i) {
        this.data[i / 64] |= (1l << (i % 64));
    }

    /**
     * @param i index
     * @param b value of the bit
     */
    public void set(final int i, final boolean b) {
        if (b)
            set(i);
        else
            unset(i);
    }

    /**
     * Set to false
     *
     * @param i index of the bit
     */
    public void unset(final int i) {
        this.data[i / 64] &= ~(1l << (i % 64));
    }

    public int xorcardinality(BitSet bs) {
        if (data.length != bs.data.length)
            throw new IllegalArgumentException("incompatible bitsets");
        int sum = 0;
        for (int k = 0; k < data.length; ++k) {
            sum += Long.bitCount(data[k] ^ bs.data[k]);
        }
        return sum;
    }

    private long[] data;

}
