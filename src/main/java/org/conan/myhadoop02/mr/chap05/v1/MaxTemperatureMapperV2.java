package org.conan.myhadoop02.mr.chap05.v1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhangzhibo on 17-5-29.
 */
public class MaxTemperatureMapperV2 extends Mapper<LongWritable,Text,Text,IntWritable>{
    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
        String line = value.toString();
        String year = line.substring(15,19);
        String tmp = line.substring(87,92);
       if (!missing(tmp))
       {
           int airTemperature = Integer.parseInt(tmp);
           context.write(new Text(year),new IntWritable(airTemperature));
       }
    }
    public boolean missing(String tmp){
        return tmp.equals("+9999");
    }
}
