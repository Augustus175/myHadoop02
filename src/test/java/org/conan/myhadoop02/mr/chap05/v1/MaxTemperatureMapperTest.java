package org.conan.myhadoop02.mr.chap05.v1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.conan.myhadoop02.mr.chap05.v2.NcdcRecordParserTest;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.io.IOException;
import java.util.Arrays;

/**
 * MaxTemperatureMapper Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>五月 29, 2017</pre>
 */
public class MaxTemperatureMapperTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: map(LongWritable key, Text value, Context context)
     */
    @Test
    public void processesValidRecord() throws IOException, InterruptedException {
//TODO: Test goes here...
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
// Year ^^^^
                "99999V0203201N00261220001CN9999999N9-00111+99999999999");
// Temperature ^^^^^
        String str = "0043011990999991950051518004+68750+023550FM-12+0382" +
// Year ^^^^
                "99999V0203201N00261220001CN9999999N9-00111+99999999999";
        System.out.println(str.substring(87, 92));
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapperV1())
                .withInput(new LongWritable(), value)
                .withOutput(new Text("1950"), new IntWritable(-11))
                .runTest();

    }

    @Test
    public void ignoresMissingTemeratureRecord() throws IOException, InterruptedException {

        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
// Year ^^^^
                "99999V0203201N00261220001CN9999999N9+99991+99999999999");
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapperV2())
                .withInput(new LongWritable(), value)
                .runTest();
    }

    @Test
    public void returnsMaximumIntegerInValues() throws IOException, InterruptedException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new MaxTemperatureReducer())
                .withInput(new Text("1905"), Arrays.asList(new IntWritable(10), new IntWritable(5)))
                .withOutput(new Text("1905"), new IntWritable(10))
                .runTest();
    }

}
