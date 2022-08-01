package com.yonyou.iot.opentsdb.reader.util;

import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public final class HttpUtils {

    public static final Charset UTF_8 = StandardCharsets.UTF_8;
    public static final int CONNECT_TIMEOUT_DEFAULT_IN_MILL = (int) TimeUnit.SECONDS.toMillis(60);
    public static final int SOCKET_TIMEOUT_DEFAULT_IN_MILL = (int) TimeUnit.SECONDS.toMillis(60);

    private HttpUtils() {
    }

    public static String get(String url) throws Exception {
        Content content = Request.Get(url)
                .connectTimeout(CONNECT_TIMEOUT_DEFAULT_IN_MILL)
                .socketTimeout(SOCKET_TIMEOUT_DEFAULT_IN_MILL)
                .execute()
                .returnContent();
        if (content == null) {
            return null;
        }
        return content.asString(UTF_8);
    }
}
