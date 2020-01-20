package com.jaslou.util;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class DataGenericUtil {
    public static void main(String[] args) throws Exception {
        generateLoginRecord(100L);
    }

    public static void generateLoginRecord(Long size) throws Exception {
        // userId, ipAddress, status, timestamp
        String[] ip = {"11", "12", "25", "13", "122", "152", "172", "9", "144"};
        String[] status = {"fail","success"};
        Random random = new Random(10000);
        StringBuilder stringBuilder = new StringBuilder();
        for (int j = 0; j < size; j++) {
            int i = random.nextInt(9);
            int a = random.nextInt(9);
            int b = random.nextInt(9);
            int c = random.nextInt(9);
            Date date = randomDate("2019-01-01","2019-01-02");
//            String dateFormat = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss").format(date);

            stringBuilder.append(date.getTime()).append("\t")
                    .append(random.nextInt(10000)).append("\t")
                    .append(ip[i] + "." + ip[a] + "." + ip[b] + "." + ip[c]).append("\t")
                    .append(status[random.nextInt(2)]).append("\n");
        }
        File file = new File("/home/hadoop/flink-project/user-behavior-analysis/src/main/resources/loginrecord.csv");
        if (!file.exists()) {
            file.createNewFile();
        }
        FileWriter fileWriter = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        bufferedWriter.write(stringBuilder.toString());
        bufferedWriter.close();
    }

    public static void generateLogindata() throws Exception {
        // ipAddress, timestamp, method, url
        String[] ip = {"11", "12", "25", "13", "122", "152", "172", "9", "144"};
        String[] url = {
                "https://tianchi.aliyun.com",
                "www.baidu.com",
                "https://ci.apache.org",
                "http://chenyuzhao.me/",
                "https://blog.csdn.net/u012369535/article/details/96317565",
                "https://www.ibm.com/",
                "https://wssp.hainan.gov.cn",
                "http://gjj.hainan.gov.cn",
                "http://ping.chinaz.com/agent.youngport.com.cn"
        };
        String[] method = {"POST","GET"};
        Random random = new Random(10);
        StringBuilder stringBuilder = new StringBuilder();
        for (int j = 0; j < 1000000; j++) {
            int i = random.nextInt(9);
            int a = random.nextInt(9);
            int b = random.nextInt(9);
            int c = random.nextInt(9);
            Date date = randomDate("2019-01-01","2019-01-02");
            String dateFormat = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss").format(date);
            stringBuilder
                    .append(ip[i] + "." + ip[a] + "." + ip[b] + "." + ip[c]).append("\t")
                    .append(dateFormat).append("\t")
                    .append(url[i]).append("\t")
                    .append(method[i%2]).append("\n");
        }
        File file = new File("F:/flink-project/flink-project/user-behavior-analysis/src/main/resources/apache.log");
        if (!file.exists()) {
            file.createNewFile();
        }
        FileWriter fileWriter = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        bufferedWriter.write(stringBuilder.toString());
        bufferedWriter.close();
    }

    private static long random(long begin,long end){
        long rtn = begin + (long)(Math.random() * (end - begin));
        if(rtn == begin || rtn == end){
            return random(begin,end);
        }
        return rtn;
    }

    private static Date randomDate(String beginDate, String endDate){
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            Date start = format.parse(beginDate);
            Date end = format.parse(endDate);

            if(start.getTime() >= end.getTime()){
                return null;
            }
            long date = random(start.getTime(),end.getTime());
            return new Date(date);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
