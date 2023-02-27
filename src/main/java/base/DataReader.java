package base;

import java.io.InputStream;

public interface DataReader<T> {
    T read(InputStream inputStream) throws Exception;
}

