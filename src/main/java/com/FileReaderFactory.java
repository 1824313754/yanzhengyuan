package com;

import read.CsvDataReader;
import base.DataReader;
import read.XlsDataReader;

import java.util.HashMap;
import java.util.Map;

public class FileReaderFactory {
    private static final Map<String, DataReader> readers = new HashMap<>();

    static {
        readers.put("csv", new CsvDataReader());
        readers.put("xls", new XlsDataReader());
    }

    @SuppressWarnings("unchecked")
    public static <T> DataReader<T> getFileReader(String fileType) {
        return (DataReader<T>) readers.get(fileType);
    }
}


