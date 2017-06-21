package org.conan.myhadoop02.mr.chap05.v2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhangzhibo on 17-5-30.
 */
public class MaxTemperatureMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    private NcdcRecordParser parser = new NcdcRecordParser();
    @Override
    public void map(LongWritable key,Text value, Context context) throws IOException,InterruptedException{
        parser.parse(value);
        if (parser.isValideTemperature()){
            context.write(new Text(parser.getYear()),new IntWritable(parser.getAirTemperature()));
        }
    }
}
