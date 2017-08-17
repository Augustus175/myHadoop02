package org.conan.myhadoop02.mr;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by zhangzhibo on 17-7-18.
 */
public class RowKeyConverter {
    private static final int STARION_ID_LENGTH = 12;
    public static byte[] makeObservationRowKey(String stationId,long observationTime){
        byte[] row  = new byte[STARION_ID_LENGTH+ Bytes.SIZEOF_LONG];
        Bytes.putBytes(row,0,Bytes.toBytes(stationId),0,STARION_ID_LENGTH);
        long reverseOderTimestamp = Long.MAX_VALUE - observationTime;
        Bytes.putLong(row,STARION_ID_LENGTH,reverseOderTimestamp);
        return row;
    }
}
