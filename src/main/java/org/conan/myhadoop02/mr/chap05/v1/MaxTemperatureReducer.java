package org.conan.myhadoop02.mr.chap05.v1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.channels.FileLockInterruptionException;

/**
 * Created by zhangzhibo on 17-5-29.
 */
public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context contest) throws IOException,InterruptedException {
        int maxValue = Integer.MIN_VALUE;
        for (IntWritable value :
                values) {
            maxValue = Math.max(value.get(),maxValue);
        }
        contest.write(key,new IntWritable(maxValue));
    }
}
