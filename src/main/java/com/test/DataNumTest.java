package com.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import javax.swing.plaf.synth.SynthOptionPaneUI;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Created by liuxinyuan on 2017/5/26.
 */
public class DataNumTest {
    public void stampToDate() {
        String res = null;
        String s = "20170423 235959";
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
                    "yyyy-MM-dd HH:mm:ss");
            long lt = new Long(s);
            Date date = new Date(lt);
            res = simpleDateFormat.format(date);
        } catch (Exception e) {
            // TODO: handle exception


        }

    }

    public static void getHdfsDirStr(String filePath) throws IOException {
        final String DELIMITER = new String(new byte[]{1});
        final String INNER_DELIMITER = ",";
        try {
            System.load("D:\\soft\\hadoop-common-2.6.0-bin\\hadoop-common-2.6.0-bin-master\\hadoop.dll");
        } catch (UnsatisfiedLinkError e) {
            System.out.println("hadoop插件載入失敗");
            System.exit(1);
        }

        System.setProperty("hadoop.home.dir", "D:\\soft\\hadoop-common-2.6.0-bin\\hadoop-common-2.6.0-bin-master\\");
        // 遍历目录下的所有文件
        FSDataInputStream br;
        FileOutputStream fw = null;
        try {
            File outFile = new File("D:\\soft\\aaa.txt");
            if (outFile.exists()) {
                outFile.delete();
            }
            fw = new FileOutputStream(outFile);
            fw.write("开始写入".getBytes());
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://hadoop-senior0.ibeifeng.com:8020");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] status = fs.listStatus(new Path(filePath));
            System.out.println(String.format("目录下面一共有%s文件:", status.length));
            String a = null;
            for (FileStatus file : status) {
                System.out.println(file.getPath().getName());
                if (file.getPath().getName().startsWith("_")) {

                    continue;
                }

                br = fs.open(file.getPath());

                byte buffer[] = new byte[1024];
                int bytesRead = 0;

              /*
               这种方式会出出现乱码
               while((bytesRead = br.read(buffer))>0){
                    fw.write(buffer);
                }
                */
                IOUtils.copyBytes(br,fw,4096,false);
                IOUtils.closeStream(br);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(fw);
        }
    }


    public static void main(String[] args) throws IOException {
        getHdfsDirStr("/output/ot3");

    }
}
