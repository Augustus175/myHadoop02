package org.conan.myhadoop02.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by zhangzhibo on 17-7-21.
 */

class TokenizerMapper extends Mapper<Object,Text,Text,IntWritable>{
    private IntWritable one = new IntWritable(1);
    public void map(Object key,Text value,Context context) throws IOException ,InterruptedException{
        Text word = new Text();
        StringTokenizer itr = new StringTokenizer(value.toString());
        while(itr.hasMoreTokens()){
           word.set(itr.nextToken());
           context.write(word,one);
        }
    }
}
class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
    public void reduce(Text word,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
        int sum = 0;
        IntWritable result = new IntWritable();
        for (IntWritable val :
                values) {
            sum+=val.get();
        }
        result.set(sum);
        context.write(word,result);
    }
}
public class WordCount {
    public static void main(String[] args) throws Exception {
        File output = new File(args[1]);
        File[] files = output.listFiles();
        for (File f :
                files) {
            new File(f.getPath()).delete();
        }
        output.delete();
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if (otherArgs.length<2){
           System.err.println("usage : <input> <output>");
           System.exit(2);
        }
        Job job = new Job();
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)? 0:1);
    }
}
