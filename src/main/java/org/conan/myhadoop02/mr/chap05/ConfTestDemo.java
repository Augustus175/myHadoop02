package org.conan.myhadoop02.mr.chap05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.net.URI;


/**
 * Created by zhangzhibo on 17-5-26.
 */
public class ConfTestDemo {
    public static void main(String[] args) {

        Path fileResource = new Path("/home/zhangzhibo/IdeaProjects/testconf/configuration-1.xml");
        Configuration conf = new Configuration();
        conf.addResource(fileResource);
        System.out.println(conf.get("color"));
        System.out.println(conf.getInt("size", 0));
        System.out.println(conf.get("breadth", "wide"));

    }
}
