package com.yonyou.iot.opentsdb.reader.util;

public final class TSDBUtils {

    private TSDBUtils() {
    }

    public static String version(String address) {
        String url = String.format("%s/api/version", address);
        String rsp;
        try {
            rsp = HttpUtils.get(url);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rsp;
    }

    public static String config(String address) {
        String url = String.format("%s/api/config", address);
        String rsp;
        try {
            rsp = HttpUtils.get(url);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rsp;
    }
}
