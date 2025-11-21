package com.meiya.whalex.graph.entity;

import java.util.Arrays;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlBytesValue
 */
public class QlBytesValue extends QlValueAdapter {

    private final byte[] val;

    public QlBytesValue(byte[] val) {
        if (val == null) {
            throw new IllegalArgumentException("Cannot construct BytesValue from null");
        } else {
            this.val = val;
        }
    }

    public boolean isEmpty() {
        return this.val.length == 0;
    }

    public int size() {
        return this.val.length;
    }

    public byte[] asObject() {
        return this.val;
    }

    public byte[] asByteArray() {
        return this.val;
    }

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.BYTES();
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            QlBytesValue values = (QlBytesValue)o;
            return Arrays.equals(this.val, values.val);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Arrays.hashCode(this.val);
    }

    public String toString() {
        StringBuilder s = new StringBuilder("#");
        byte[] var2 = this.val;
        int var3 = var2.length;

        for(int var4 = 0; var4 < var3; ++var4) {
            byte b = var2[var4];
            if (b < 16) {
                s.append('0');
            }

            s.append(Integer.toHexString(b));
        }

        return s.toString();
    }

}
