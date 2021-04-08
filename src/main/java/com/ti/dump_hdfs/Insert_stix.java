package com.ti.dump_hdfs;

import com.ti.dump_hdfs.util.ProduceHdfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Insert_stix {
    private  ProduceHdfs produceHdfs;

    private  void dump_hdfs(String hdfsPath,String localPath) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), configuration);
        FSDataOutputStream out = fs.create(new Path(hdfsPath));//创建一个输出流
        InputStream in = new FileInputStream(new File(localPath));//从本地读取文件
        IOUtils.copyBytes(in, out, 100, true);
    }

    private  void  submitStixDirectory(String stix_directory,String hdfs_directory) throws IOException {
        produceHdfs=new ProduceHdfs();
        File file=new File(stix_directory);
        String new_hdfs_directory=hdfs_directory+getCurrentTime()+"/";
        if(file.exists())
        {
            File[] fs=file.listFiles();
            for(File f:fs)
            {
                String name=f.getName();
                if(!f.isDirectory()&&name.endsWith("json"))
                {
                    dump_hdfs(new_hdfs_directory+name,f.getPath());
                }
            }
            System.out.println(stix_directory+",上传完毕");
            if(fs.length>0)
            {
                produceHdfs.produceHdfsPath(new_hdfs_directory);
            }
        }
    }
    public static String getCurrentTime()
    {
        Date date=new Date();
        SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd_HH.mm.ss");
        String format_date=simpleDateFormat.format(date);
        System.out.println(format_date);
        return  format_date;
    }

    public static void main(String[] args) throws IOException {
        new Insert_stix().submitStixDirectory(args[0],args[1]);
    }
}
