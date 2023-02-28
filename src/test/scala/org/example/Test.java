package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class Test {
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        InputStream inputStream = new Test().getClass().getResourceAsStream("/config.properties");
        BufferedReader bf = new BufferedReader(new InputStreamReader(inputStream));
        properties.load(bf);
        System.out.println(properties.getProperty("China"));
    }
}
