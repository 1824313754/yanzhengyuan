package base;

import com.FileReaderFactory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileFilter;
import org.joda.time.DateTime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;


public abstract class AbstractFtpFileProcessor<T> {
    protected FTPClient ftpClient;
    protected Connection connection;
    private Set<String> processedFileSet = new HashSet<>();
    //定义一个数组，存放csv、xls、xlsx
    private String[] fileType = {"csv", "xls", "xlsx"};
    //所有公共的配置信息
    Map<String, Map<String, String>> configData = new HashMap<>();
    //自定义的配置信息
    Map<String, Map<String, String>> resultMap = new HashMap<>();
    //文件大小
    private long fileSize;
    //获取当前时间戳，转为YYYY-MM-DD HH:MM:SS格式
    private String date = new DateTime().toString("yyyy-MM-dd HH:mm:ss");

    public AbstractFtpFileProcessor(Properties config) throws SQLException {
        String mysqlUrl = config.getProperty("mysql.url");
        String mysqlUser = config.getProperty("mysql.user");
        String mysqlPassword = config.getProperty("mysql.password");
        connection = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
        getStandardConfigData();
    }

    public void process(Properties config, String fileType) throws Exception {
        String server = config.getProperty("ftp.server");
        int port = Integer.parseInt(config.getProperty("ftp.port"));
        String username = config.getProperty("ftp.username");
        String password = config.getProperty("ftp.password");
        String remoteFilePath = config.getProperty("ftp.remoteFilePath");
        ftpClient = new FTPClient();
        ftpClient.connect(server, port);
        ftpClient.login(username, password);
        ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
        ftpClient.setControlEncoding(StandardCharsets.UTF_8.name());
        //设置被动模式
        ftpClient.enterLocalPassiveMode();
        try {
            ftpClient.changeWorkingDirectory(new String(remoteFilePath.getBytes(StandardCharsets.UTF_8), "iso-8859-1"));

            List<String> filePaths = listFilesAndDirectories();
            for (String filePath : filePaths) {
                String ext = filePath.substring(filePath.lastIndexOf(".") + 1).toLowerCase();
                if (ext.equals(fileType)) {
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    try {
                        boolean success = ftpClient.retrieveFile(new String(filePath.getBytes(StandardCharsets.UTF_8), "iso-8859-1"), outputStream);
                        if (success) {
                            byte[] fileData = outputStream.toByteArray();
                            fileSize = fileData.length;
                            // 根据文件类型获取文件读取器对象
                            DataReader<T> dataReader = FileReaderFactory.getFileReader(ext);
                            T data = dataReader.read(new ByteArrayInputStream(fileData));
                            doProcess(data, filePath, configData);
                            //处理成功后存入mysql
                            insertProcessedFilePath(filePath, true);
                        }
                    } catch (Exception e) {
                        insertProcessedFilePath(filePath, false);
                    }
                    outputStream.close();
                }
            }
        } finally {
            ftpClient.disconnect();
            connection.close();
        }
    }

    private void insertProcessedFilePath(String filePath, Boolean flag) throws SQLException {
        String sql = "INSERT INTO yanzhen_pathprocess (path_name,processtime,size,flag) VALUES (?, ?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, filePath);
            statement.setString(2, date);
            statement.setLong(3, fileSize);
            statement.setBoolean(4, flag);
            statement.executeUpdate();
            processedFileSet.add(filePath);
        }
    }

//    private void insertUnprocessedFilePaths(List<String> filePaths) throws SQLException {
//        Set<String> unprocessedFileSet = new HashSet<>(filePaths);
//        unprocessedFileSet.removeAll(processedFileSet);
//        if (!unprocessedFileSet.isEmpty()) {
//            String sql = "INSERT INTO yanzhen_pathprocess (path_name, flag,processtime,size) VALUES (?, false,?,?)";
//            try (PreparedStatement statement = connection.prepareStatement(sql)) {
//                for (String filePath : unprocessedFileSet) {
//                    statement.setString(1, filePath);
//                    statement.setString(2, date);
//                    statement.setLong(3, fileSize);
//                    statement.executeUpdate();
//                }
//            }
//        }
//    }

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
            FTPFile[] files = ftpClient.mlistDir(new String(path.getBytes(StandardCharsets.UTF_8), "iso-8859-1"), new FTPFileFilter() {
                @Override
                public boolean accept(FTPFile file) {
                    String ext = file.getName().substring(file.getName().lastIndexOf(".") + 1).toLowerCase();
                    if (Arrays.asList(fileType).contains(ext) || file.isDirectory()) {
                        return true;
                    }
                    return false;
                }
            });
            for (FTPFile file : files) {
                String subPath = path + "/" + file.getName();
                if (file.isDirectory()) {
                    stack.push(subPath);
//                    filesAndDirectories.add(subPath);
                } else if (file.isFile() && !processedFileSet.contains(subPath)) {
//                    System.out.println(subPath);
                    filesAndDirectories.add(subPath);
                }
            }
        }
        return filesAndDirectories;
    }

//    public void getConfigData() throws SQLException {
//        String query = "SELECT * FROM yanzheng_config ";
//
//        try (PreparedStatement preparedStatement = connection.prepareStatement(query);
//             ResultSet resultSet = preparedStatement.executeQuery()) {
//
//            ResultSetMetaData metaData = resultSet.getMetaData();
//            int columnCount = metaData.getColumnCount();
//
//            while (resultSet.next()) {
//                String[] row = new String[columnCount];
//
//                for (int i = 1; i <= columnCount; i++) {
//                    row[i - 1] = resultSet.getString(i);
//                }
//                configData.add(row);
//            }
//        }
//    }


    //查询标准化的配置信息公共字段
    public void getStandardConfigData() throws SQLException {
//        String query = "SELECT equipment, custom_field FROM yanzheng_config WHERE type='公共'";
//        Statement statement = connection.createStatement();
//        ResultSet resultSet = statement.executeQuery(query);
//        while (resultSet.next()) {
//            String equipment = resultSet.getString("equipment");
//            String customField = resultSet.getString("custom_field");
//            if (!configData.containsKey(equipment)) {
//                configData.put(equipment, new ArrayList<>());
//            }
//            configData.get(equipment).add(customField);
//        }
        String query = "SELECT standard_field, equipment, custom_field FROM yanzheng_config WHERE type='公共'";

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        while (resultSet.next()) {
            String standardField = resultSet.getString("standard_field");
            String equipment = resultSet.getString("equipment");
            String customField = resultSet.getString("custom_field");
            if (!configData.containsKey(equipment)) {
                configData.put(equipment, new HashMap<>());
            }
            Map<String, String> groupMap = configData.get(equipment);
            groupMap.put(customField, standardField);
        }

    }

    //查询标准化的配置信息自定义字段
    public void getStandardConfigData(String equipmentName) throws SQLException {
        String query = "SELECT standard_field, equipment, custom_field FROM yanzheng_config WHERE type='自定义' and equipment like '" + equipmentName + "%'";

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                String standardField = resultSet.getString("standard_field");
                String equipment = resultSet.getString("equipment");
                String customField = resultSet.getString("custom_field");
                String[] equipmentArray = equipment.split("_");
                String groupKey = equipmentArray[0];
                if (!resultMap.containsKey(groupKey)) {
                    resultMap.put(groupKey, new HashMap<>());
                }
                Map<String, String> groupMap = resultMap.get(groupKey);
                groupMap.put(customField, standardField);
            }
    }


    protected abstract void doProcess(T data, String path, Map<String, Map<String, String>> configData) throws Exception;
}
