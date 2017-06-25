package org.conan.myhadoop02.mr.chap05.v2;

import org.apache.hadoop.io.Text;

/**
 * Created by zhangzhibo on 17-5-30.
 */
public class NcdcRecordParser {
    private static final int MISSING_TEMPERATURE = 9999;

    private String year;
    private int airTemperature;
    private String quality;
    private int StationID;

    private String airTemperatureString;


    public void parse(String record) {
        year = record.substring(15, 19);

        if (record.charAt(87) == '+') {
            airTemperatureString = record.substring(88, 92);
        } else {
            airTemperatureString = record.substring(87, 92);
        }
        airTemperature = Integer.parseInt(airTemperatureString);
        quality = record.substring(92, 93);
        StationID = (int) (airTemperature * (Math.random() * 100));
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public boolean isValideTemperature() {
        return airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
    }

    public String getYear() {
        return year;
    }

    public int getYearInt() {
        return Integer.parseInt(this.year);
    }

    public String getStationID() {
        return String.valueOf(StationID);
    }

    public int getAirTemperature() {
        return airTemperature;
    }

    public String getAirTemperatureString() {
        return this.airTemperatureString;
    }

    public String getQuality() {
        return quality;
    }
}
