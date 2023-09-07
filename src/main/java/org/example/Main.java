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

    private static List<String> measurement = new ArrayList<>();

    private static List<String> deviceIds = new ArrayList<>();

    static {
        for (int i = 0; i < 500; i++) {
            measurement.add("s"+i);
        }

        deviceIds = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            deviceIds.add("root.sg.dev"+i);
        }
    }

    public static void main(String[] args) throws IOException {
        Map<String, Object> map = beforeStart();
        if(MapUtils.isEmpty(map)){
            sessionPools.add(new SessionPool("127.0.0.1", 6667, "root", "root",2));
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
                long currentTimeMillis = System.currentTimeMillis();
                zaoshuju(pools,currentTimeMillis);
                Thread.sleep(50);
            } catch (Throwable e) {
                System.out.println(e.getMessage());
                try {
                    Thread.sleep(100*5);
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

    private static void zaoshuju(List<SessionPool> pools,long currentTimeMillis) throws Exception {
        Random rand = new Random();
        List<List<String>> measurements = new ArrayList<>();
        List<Long> times = new ArrayList<>();
        for (int j = 0; j < 100; j++) {
            measurements.add(measurement);
            times.add(currentTimeMillis);
        }
        List<List<String>> valuesList = new ArrayList<>();
        for (int i = 0; i < 100 ; i++) {
            List<String> values = new ArrayList<>();
            valuesList.add(values);
            for (int k = 0; k < 500; k++) {
                values.add(String.valueOf(rand.nextFloat() % 10));
                values.add(String.valueOf(rand.nextInt() % 2 + 3));
                values.add(String.valueOf(rand.nextDouble() % 10));
                values.add(String.valueOf(rand.nextBoolean()));
                values.add(String.valueOf(rand.nextFloat() % 10));
            }
        }

        for (SessionPool pool : pools) {
            pool.insertRecords(
                    deviceIds,times, measurements, valuesList);
        }
    }

//    public static void main(String[] args) throws Exception {
//
//    MQTT mqtt = new MQTT();
//    mqtt.setHost("tcp://172.20.31.76:1883");
//    mqtt.setClientId("");
//
//    mqtt.setVersion("3.1.1");
//    mqtt.setConnectAttemptsMax(30);
//    mqtt.setReconnectDelay(10);
//    mqtt.setReconnectAttemptsMax(6);
//
//    BlockingConnection connection = mqtt.blockingConnection();
//    connection.connect();
//
//    Random random = new Random();
////        StringBuilder sb = new StringBuilder();
//        for (int i = 0; i < 10000; i++) {
//            String payload =
//                    String.format(
//                            "{\n"
//                                    + "\"device\":\"root.sg.dev0\",\n"
//                                    + "\"timestamp\":%d,\n"
//                                    + "\"measurements\":[\"sp\"],\n"
//                                    + "\"values\":[%f]\n"
//                                    + "}",
//                            System.currentTimeMillis(), random.nextFloat());
////            String payload =
////                    String.format(
////                            "{\n"
////                                    + "\"tag\":\"root.sg.dev0\",\n"
////                                    + "\"timestamp\":%d,\n"
////                                    + "\"values\":%f,\n"
////                                    + "\"quality\":0\n"
////                                    + "}",
////                            System.currentTimeMillis(), random.nextFloat());
////            sb.append(payload).append(",");
//
//            // publish a json object
////            Thread.sleep(1);
////            connection.publish("rtdb.root.sg.dev0.s0", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
////        }
//            // publish a json array
////        sb.insert(0, "[");
////        sb.replace(sb.lastIndexOf(","), sb.length(), "]");
//            connection.publish("root/sg/dev0", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
//            Thread.sleep(500);
//        }
//        connection.disconnect();
//    }
}