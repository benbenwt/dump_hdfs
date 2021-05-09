package com.ti.dump_hdfs;

import java.io.File;

public class test {
    public static void main(String[] args) {
        String path="C:/Users/guo/Desktop/input1";
        File file=new File(path);
        System.out.println(path);
        System.out.println(file.exists());
    }
}
