package org.conan.myhadoop02.mr.chap08;

/**
 * Created by zhangzhibo on 17-6-22.
 */
public class pathtest {
    public static void main(String[] args) {
        System.out.println(System.getProperty("java.library.path"));
        System.out.println("Java类路径：" + System.getProperty("java.class.path"));
    }
}
