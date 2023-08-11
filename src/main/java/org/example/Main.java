package org.example;

import org.apache.commons.collections4.MapUtils;
import org.apache.iotdb.session.pool.SessionPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
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
            sessionPools.add(new SessionPool("172.20.31.56", 6667, "root", "root",2));
            execute(sessionPools);
        }else {
            String urls = String.valueOf(map.get("urls"));
            String[] split = urls.split(";");
            String usernames = String.valueOf(map.get("username"));
            String[] names = usernames.split(";");
            String passwords = String.valueOf(map.get("password"));
            String[] pwds = passwords.split(";");
            for (int i = 0; i < split.length; i++) {
                String[] url = split[i].split(":");
                sessionPools.add(new SessionPool(url[0],Integer.valueOf(url[1]) , names[i], pwds[i],2));
            }
            execute(sessionPools);
        }
    }

    private static void execute(List<SessionPool> pools) {
        while (true) {
            try {
                zaoshuju(pools);
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
        HashMap<String, Object> map = new HashMap<>();
        InputStream inputStream = ClassLoader.getSystemResourceAsStream("config.properties");
        Properties properties = new Properties();
        properties.load(inputStream);
        properties.list(System.out);
        map.put("urls",properties.get("urls"));
        map.put("username",properties.get("username"));
        map.put("password",properties.get("password"));
        return map;
    }

    private static void zaoshuju(List<SessionPool> pools) throws Exception {
        List<String> measurement = new ArrayList<>();
        measurement.add("L1_BidPrice");
        measurement.add("Type");
        measurement.add("L1_BidSize");
        measurement.add("Domain");
        measurement.add("L1_BuyNo");
        measurement.add("L1_AskPrice");
        List<String> values = new ArrayList<>();
        Random rand = new Random();
        values = new ArrayList<>();
        values.add(String.valueOf(rand.nextFloat() % 10));
        values.add(String.valueOf(rand.nextInt() % 2 + 3));
        values.add(String.valueOf(rand.nextDouble() % 10));
        values.add(String.valueOf(rand.nextInt() % 2 - 5));
        values.add(String.valueOf(rand.nextBoolean()));
        values.add(String.valueOf(rand.nextFloat() % 10));
        for (SessionPool pool : pools) {
            pool.insertRecord(
                    "root.stock.Legacy.0700HK", System.currentTimeMillis(), measurement, values);
        }
    }
}