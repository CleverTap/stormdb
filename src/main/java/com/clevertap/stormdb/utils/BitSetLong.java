package com.clevertap.stormdb.utils;


import gnu.trove.procedure.TLongProcedure;
import java.util.ArrayList;
import java.util.BitSet;

public class BitSetLong implements BitSetList {

    ArrayList<BitSet> bitSetList;

    public BitSetLong() {
        bitSetList = new ArrayList<>();
    }

    @Override
    public void ensureIdx(int idx) {
        if (idx >= bitSetList.size()) {
            while (bitSetList.size() < idx + 1) {
                bitSetList.add(null);
            }
        }

        if (bitSetList.get(idx) == null) {
            bitSetList.set(idx, new BitSet());
        }
    }

    @Override
    public void set(long long_uid) {
        int idx = (int) (long_uid / MAX_BITS_IN_BIT_SET);
        ensureIdx(idx);
        int toAdd = (int) (long_uid % MAX_BITS_IN_BIT_SET);
        bitSetList.get(idx).set(toAdd);
    }

    @Override
    public boolean get(long long_uid) {
        int idx = (int) (long_uid / MAX_BITS_IN_BIT_SET);
        return idx < bitSetList.size() && bitSetList.get(idx) != null
                && bitSetList.get(idx).get((int) (long_uid % MAX_BITS_IN_BIT_SET));
    }

    public void unset(long long_uid) {
        int idx = (int) (long_uid / MAX_BITS_IN_BIT_SET);
        if (get(long_uid)) {
            bitSetList.get(idx).set((int) (long_uid % MAX_BITS_IN_BIT_SET), false);
        }
    }

    public void forEach(TLongProcedure t) {
        long i = 0;
        for (BitSet bitSet : bitSetList) {
            if (bitSet != null) {
                int b = bitSet.nextSetBit(0);
                while (b != -1) {
                    t.execute(b + (i * MAX_BITS_IN_BIT_SET));
                    b = bitSet.nextSetBit(b + 1);
                }
            }
            i++;
        }
    }

    // TODO Fix this, the old version #toByteArrayOld() is correct but causes OOM
    // this is temp fix to supress OOM
    public byte[] toByteArray() {
        if (bitSetList == null || bitSetList.isEmpty() || bitSetList.get(0) == null) {
            return new byte[0];
        }
        return bitSetList.get(0).toByteArray();
    }
}
