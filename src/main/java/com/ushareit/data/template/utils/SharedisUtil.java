package com.ushareit.data.template.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class SharedisUtil {
    public static final int kSegmentSize = 1;
    public static final int kKeyMetaSize = 2;
    public static final int kKeySizeLengthInBits = 12;

    // type: KV=0, DEDUP_SET=2, HASH=4
    public static ByteBuffer encodeKey(String segment, byte type, String rawkey, String member) {
        short segmentSize = (short)segment.getBytes().length;
        short metaSize = (short)rawkey.getBytes().length;
        short memberSize = 0;
        if (StringUtils.isNotEmpty(member)) {
            memberSize = (short)member.getBytes().length;
        }
        int rowKeySize = kSegmentSize + segmentSize + kKeyMetaSize + metaSize + memberSize;

        ByteBuffer buffer =  ByteBuffer.allocate(rowKeySize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        
        // encode segment
        buffer.put((byte)segmentSize);
        buffer.put(segment.getBytes());

        // encode key meta data
        short flags = (short)(((type & 0x0F) << kKeySizeLengthInBits)
                | (metaSize & 0x0FFF));
        buffer.putShort(flags);

        // encode key, member
        buffer.put(rawkey.getBytes());
        if (null == member || member.isEmpty()) {
            return buffer;
        }

        buffer.put(member.getBytes());
        return buffer;
    }

    public static ByteBuffer encodeKey(Row row) {
        String segment = row.getAs("segment");
        StructField[] fields = row.schema().fields();
        byte type = 0;
        String member = "";
        for (StructField field : fields){
            String name = field.name();
            if (name.equals("storeType")){
                type = row.getAs("storeType");
                continue;
            }
            if (name.equals("storetype")){
                type = row.getAs("storetype");
                continue;
            }
            if (name.equals("type")) {
                type = row.getAs("type");
                continue;
            }
            if (name.equals("member")) {
                member = row.getAs("member");
            }
        }
        String key = row.getAs("key");
        return encodeKey(segment, type, key, member);
    }


}
