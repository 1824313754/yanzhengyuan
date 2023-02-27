package base;

import java.time.LocalDateTime;
import java.util.List;

public interface DatabaseService {
    void logProcessedFile(String filePath, boolean success, LocalDateTime processTime) throws Exception;
    List<String> getProcessedFiles() throws Exception;
}
