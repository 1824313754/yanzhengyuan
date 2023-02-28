package base;
import com.FileReaderFactory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.joda.time.DateTime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;


public abstract class AbstractFtpFileProcessor<T> {
    protected FTPClient ftpClient;
    private Connection connection;
    private Set<String> processedFileSet = new HashSet<>();
    //获取当前时间戳，转为YYYY-MM-DD HH:MM:SS格式
    private String date = new DateTime().toString("yyyy-MM-dd HH:mm:ss");

    public AbstractFtpFileProcessor(String mysqlUrl, String mysqlUser, String mysqlPassword) throws SQLException {
        connection = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
    }

    public void process(String server, int port, String username, String password, String remoteFilePath, String fileType) throws Exception {
        ftpClient = new FTPClient();
        ftpClient.connect(server, port);
        ftpClient.login(username, password);
        ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
        ftpClient.setControlEncoding(StandardCharsets.UTF_8.name());

        try {
            ftpClient.changeWorkingDirectory(new String(remoteFilePath.getBytes(StandardCharsets.UTF_8), "iso-8859-1"));
            List<String> filePaths = listFilesAndDirectories();

            for (String filePath : filePaths) {
                String ext = filePath.substring(filePath.lastIndexOf(".") + 1).toLowerCase();
                if (ext.equals(fileType)) {
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    boolean success = ftpClient.retrieveFile(new String(filePath.getBytes(StandardCharsets.UTF_8), "iso-8859-1"), outputStream);
                    if (success) {
                        byte[] fileData = outputStream.toByteArray();
                        // 根据文件类型获取文件读取器对象
                        DataReader<T> dataReader = FileReaderFactory.getFileReader(ext);
                        T data = dataReader.read(new ByteArrayInputStream(fileData));
                        doProcess(data);
                        //处理成功后存入mysql
                        insertProcessedFilePath(filePath);
                    }
                    outputStream.close();
                }
            }

            insertUnprocessedFilePaths(filePaths);
        } finally {
            ftpClient.disconnect();
            connection.close();
        }
    }

    private void insertProcessedFilePath(String filePath) throws SQLException {
        String sql = "INSERT INTO yanzhen_pathprocess (path_name, flag,processtime) VALUES (?, true,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, filePath);
            statement.setString(2, date);
            statement.executeUpdate();
            processedFileSet.add(filePath);
        }
    }

    private void insertUnprocessedFilePaths(List<String> filePaths) throws SQLException {
        Set<String> unprocessedFileSet = new HashSet<>(filePaths);
        unprocessedFileSet.removeAll(processedFileSet);
        if (!unprocessedFileSet.isEmpty()) {
            String sql = "INSERT INTO yanzhen_pathprocess (path_name, flag,processtime) VALUES (?, false,?)";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                for (String filePath : unprocessedFileSet) {
                    statement.setString(1, filePath);
                    statement.setString(2, date);
                    statement.executeUpdate();
                }
            }
        }
    }

    protected List<String> listFilesAndDirectories() throws IOException, SQLException {
        List<String> filesAndDirectories = new ArrayList<>();
        Stack<String> stack = new Stack<>();
        String pwd = ftpClient.printWorkingDirectory();
        pwd = new String(pwd.getBytes("iso-8859-1"), StandardCharsets.UTF_8);
        stack.push(pwd);

        //获取文件处理表中最新处理时间对应的flag为true的文件路径
        String sql = "SELECT t.path_name\n" +
                "FROM yanzhen_pathprocess t\n" +
                "WHERE t.processtime = (\n" +
                "  SELECT MAX(sub_t.processtime)\n" +
                "  FROM yanzhen_pathprocess sub_t\n" +
                "  WHERE sub_t.path_name = t.path_name\n" +
                ") and t.flag=true;\n";
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                String processedPath = resultSet.getString("path_name");
                processedFileSet.add(processedPath);
            }
        }

        while (!stack.isEmpty()) {
            String path = stack.pop();
            FTPFile[] files = ftpClient.listFiles(new String(path.getBytes(StandardCharsets.UTF_8), "iso-8859-1"));
            for (FTPFile file : files) {
                String subPath = path + "/" + file.getName();
                if (file.isDirectory() && !file.getName().equals(".") && !file.getName().equals("..")) {
                    stack.push(subPath);
//                    filesAndDirectories.add(subPath);
                } else if (file.isFile() && !processedFileSet.contains(subPath)) {
                    filesAndDirectories.add(subPath);
                }
            }
        }
        return filesAndDirectories;
    }

    protected abstract void doProcess(T data) throws Exception;
}
