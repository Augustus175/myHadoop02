package org.conan.myhadoop02.mr.chap05.v1; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After; 

/** 
* MaxTemperatureDriver Tester. 
* 
* @author <Authors name> 
* @since <pre>五月 31, 2017</pre> 
* @version 1.0 
*/ 
public class MaxTemperatureDriverTest { 

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: run(String[] args) 
* 
*/ 
@Test
public void testRun() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: main(String[] args) 
* 
*/ 
@Test
public void testMain() throws Exception { 
//TODO: Test goes here...
    Configuration  conf = new Configuration();
    conf.set("fs.default.name","file:///");
    conf.set("maped.job.tracker","local");

    Path input= new Path("/home/zhangzhibo/NCDCData");
    Path output = new Path("/home/zhangzhibo/NCDCData/out");
    FileSystem fs = FileSystem.get(conf);
    fs.delete(output,true);

    MaxTemperatureDriver driver = new MaxTemperatureDriver();
    driver.setConf(conf);
    int exitCode = driver.run(new String[] {
            input.toString(),output.toString()
    });
    assertThat(exitCode,is(0));

//    checkOutput(conf,output);

}


} 
