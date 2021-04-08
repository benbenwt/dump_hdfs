package com.ti.dump_hdfs;

import java.text.SimpleDateFormat;
import java.util.Date;

public class test {
    public static void main(String[] args) {
        Date date=new Date();
        SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
        String format_date=simpleDateFormat.format(date);
        System.out.println(format_date);
    }
}
