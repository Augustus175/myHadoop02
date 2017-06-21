package org.conan.myhadoop02.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by zhangzhibo on 17-6-20.
 */
public class JobBuilder {
    private final Class<?> driverClass;
    private final Job job;
    private final String extrArgsUsage;
    private final int extrArgCount;
    private String[] extraArgs;


    public JobBuilder(Class<?> driverClass) throws IOException {
        this(driverClass, 0, "");
    }

    public JobBuilder(Class<?> driverClass, int extrArgCount, String extrArgsUsage) throws IOException {
        this.driverClass = driverClass;
        this.extrArgsUsage = extrArgsUsage;
        this.extrArgCount = extrArgCount;
        this.job = new Job();
        this.job.setJarByClass(driverClass);
    }

    public static Job parseInputAndOutput(Tool tool, Configuration conf, String[] args) throws IOException {
        if (args.length != 2) {
            printUsage(tool, "<input> <output>");
            return null;
        }
        Job job = new Job(conf);
        job.setJarByClass(tool.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job;
    }

    public static void printUsage(Tool tool, String extrArgsUsage) {
        System.err.printf("Usage: %s [genericOptions] %s \n\n", tool.getClass().getSimpleName(), extrArgsUsage);
        GenericOptionsParser.printGenericCommandUsage(System.err);
    }

    public JobBuilder withCommandLinerArgs(String... args) throws IOException {
        Configuration conf = job.getConfiguration();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        String[] otherArgs = parser.getRemainingArgs();
        if (otherArgs.length < 2 && otherArgs.length > 3 + extrArgCount) {
            System.err.printf("Usage: %s [genericOptions] [-overwrite] <input path> <output> %s \n\n", driverClass.getSimpleName(), extrArgsUsage);
            GenericOptionsParser.printGenericCommandUsage(System.err);
            System.exit(-1);
        }
        int index = 0;
        boolean overwrite = false;
        if (otherArgs[index].equals("-overwrite")) {
            overwrite = true;
            index++;
        }
        Path input = new Path(otherArgs[index++]);
        Path output = new Path(otherArgs[index++]);
        if (index<otherArgs.length){
            extraArgs = new String[otherArgs.length-index];
            System.arraycopy(otherArgs,index,extraArgs,0,otherArgs.length-index);
        }
        if(overwrite){
            output.getFileSystem(conf).delete(output,true);
        }
        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);
        return this;
    }
    public Job build(){
        return job;
    }
    public String[] getExtraArgs(){
        return extraArgs;
    }
}
