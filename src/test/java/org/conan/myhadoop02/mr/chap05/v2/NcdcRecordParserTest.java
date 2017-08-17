package org.conan.myhadoop02.mr.chap05.v2;

import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import static org.junit.Assert.assertEquals;

/**
 * NcdcRecordParser Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>五月 30, 2017</pre>
 */
public class NcdcRecordParserTest {

    //    NcdcRecordParserTest parser = new NcdcRecordParserTest();
    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: parse(String record)
     */
    @Test
    public void testParseRecord() throws Exception {
//TODO: Test goes here...
        NcdcRecordParser parser = new NcdcRecordParser();
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
// Year ^^^^
                "99999V0203201N00261220001CN9999999N9+99991+99999999999");
        parser.parse(value);
        assertEquals(9999, parser.getAirTemperature());
    }

    /**
     * Method: isValideTemperature()
     */
    @Test
    public void testIsValideTemperature() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getYear()
     */
    @Test
    public void testGetYear() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getAirTemperature()
     */
    @Test
    public void testGetAirTemperature() throws Exception {
//TODO: Test goes here... 
    }


} 
