package org.example;

import org.apache.commons.collections4.MapUtils;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class Main {

    private static List<SessionPool> sessionPools = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        Map<String, Object> map = beforeStart();
        if(MapUtils.isEmpty(map)){
            sessionPools.add(new SessionPool("127.0.0.1", 6667, "root", "root",2));
            String[] measurements = "L1_BidPrice;Type;L1_BidSize;Domain;L1_BuyNo;L1_AskPrice".split(";");
            String[] types = "int;float;double;boolean;String;long".split(";");
            execute(sessionPools,"root.stock.Legacy.0700HK",measurements,types);
        }else {
            String urls = String.valueOf(map.get("urls"));
            String[] split = urls.split(";");
            String usernames = String.valueOf(map.get("username"));
            String[] names = usernames.split(";");
            String passwords = String.valueOf(map.get("password"));
            String[] pwds = passwords.split(";");
            String deviceId = String.valueOf(map.get("deviceId"));
            String measurement = String.valueOf(map.get("measurements"));
            String[] measurements = measurement.split(";");
            String dataType = String.valueOf(map.get("dataType"));
            String[] types = dataType.split(";");
            for (int i = 0; i < split.length; i++) {
                String[] url = split[i].split(":");
                sessionPools.add(new SessionPool(url[0],Integer.valueOf(url[1]) , names[i], pwds[i],2));
            }
            execute(sessionPools,deviceId,measurements,types);
        }
    }

    private static void execute(List<SessionPool> pools,String deviceId,String[] measurements,String[] types) {
        while (true) {
            try {
                zaoshuju(pools,deviceId,measurements,types);
                Thread.sleep(1000);
            } catch (Throwable e) {
                System.out.println(e.getMessage());
                try {
                    Thread.sleep(1000*5);
                } catch (InterruptedException ex) {
                    System.out.println("sleep exep:"+e.getMessage());
                }
            }
        }
    }

    private static Map<String, Object> beforeStart() throws IOException {
        String filePath = System.getProperty("user.dir") + "/config.properties";
        HashMap<String, Object> map = new HashMap<>();
        Properties properties = new Properties();
        properties.load(Main.class.getClassLoader().getResourceAsStream("config.properties"));
        if(new File(filePath).exists()) {
            properties.load(new FileReader(filePath));
        }
        properties.list(System.out);
        map.put("urls",properties.get("urls"));
        map.put("username",properties.get("username"));
        map.put("password",properties.get("password"));
        map.put("deviceId",properties.get("deviceId"));
        map.put("measurements",properties.get("measurements"));
        map.put("dataType",properties.get("dataType"));
        return map;
    }

    private static void zaoshuju(List<SessionPool> pools,String deviceId,String[] measurements,String[] types) throws Exception {
        List<Object> values = new ArrayList<>();
        List<TSDataType> typeList = new ArrayList<>();
        Random rand = new Random();
        for (int i = 0; i < types.length; i++) {
            String type = types[i];
            switch (type.toLowerCase()){
                case "double" :values.add(rand.nextDouble() % 10);typeList.add(TSDataType.DOUBLE);break;
                case "int" :values.add(rand.nextInt() % 2 - 5);typeList.add(TSDataType.INT32);break;
                case "long" :values.add(rand.nextLong());typeList.add(TSDataType.INT64);break;
                case "float" :values.add(rand.nextFloat() % 10);typeList.add(TSDataType.FLOAT);break;
                case "boolean" :values.add(rand.nextBoolean());typeList.add(TSDataType.BOOLEAN);break;
                default:
                    values.add(String.valueOf(rand.nextInt()));typeList.add(TSDataType.TEXT);break;
            }

        }
        for (SessionPool pool : pools) {
            pool.insertRecord(deviceId, System.currentTimeMillis(), Arrays.asList(measurements),typeList, values);
        }
    }
}