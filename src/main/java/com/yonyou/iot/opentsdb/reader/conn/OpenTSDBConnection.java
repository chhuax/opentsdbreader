package com.yonyou.iot.opentsdb.reader.conn;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

public class OpenTSDBConnection{

    private final String address;

    public OpenTSDBConnection(String address) {
        this.address = address;
    }
    public String config() throws IOException {
        File file = Paths.get(this.address).toFile();
        return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
    }
}
