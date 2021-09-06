package com.jaslou.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class PropertyUtil {
    private final static String CONF_NAME = "config.properties";

    private static Properties contextProperties;

    static {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(CONF_NAME);
        contextProperties = new Properties();
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(in, "UTF-8");
            contextProperties.load(inputStreamReader);
        } catch (IOException e) {
            System.err.println(">>>config.properties<<<资源文件加载失败!");
            e.printStackTrace();
        }
        System.out.println(">>>config.properties<<<资源文件加载成功");
    }

    public static String getStrValue(String key) {
        return contextProperties.getProperty(key);
    }

    public static int getIntValue(String key) {
        String strValue = getStrValue(key);
        // 注意，此处没有做校验，暂且认为不会出错
        return Integer.parseInt(strValue);
    }

    /***
     * get kafka configure properties
     * @return
     */
    public static Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", getStrValue("kafka.bootstrap.servers"));
        properties.setProperty("zookeeper.connect", getStrValue("kafka.zookeeper.connect"));
        properties.setProperty("group.id", getStrValue("kafka.zookeeper.group.id"));
        return properties;
    }

    /***
     * get resources config file Properties
     * @param config_file
     * @return
     */
    public static Properties getProperties(String config_file) {
        Properties properties = new Properties();
        InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream(config_file);
        try  {
            InputStreamReader reader = new InputStreamReader(input, "UTF-8");
            properties.load(reader);
        } catch (IOException e) {
        System.err.println( ">>>+" + config_file +"<<<资源文件加载失败!");
        e.printStackTrace();
        }
        return properties;
    }

    public static Properties getProperties() {
        return contextProperties;
    }
}
