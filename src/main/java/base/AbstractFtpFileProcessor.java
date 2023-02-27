package base;
import com.FileReaderFactory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public abstract class AbstractFtpFileProcessor<T> {
    protected FTPClient ftpClient;

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
                    }
                    outputStream.close();
                }
            }
        } finally {
            ftpClient.disconnect();
        }
    }

    protected List<String> listFilesAndDirectories() throws IOException {
        List<String> filesAndDirectories = new ArrayList<>();
        Stack<String> stack = new Stack<>();
        String pwd = ftpClient.printWorkingDirectory();
        pwd = new String(pwd.getBytes("iso-8859-1"), StandardCharsets.UTF_8);
        stack.push(pwd);
        while (!stack.isEmpty()) {
            String path = stack.pop();
            FTPFile[] files = ftpClient.listFiles(new String(path.getBytes(StandardCharsets.UTF_8), "iso-8859-1"));
            for (FTPFile file : files) {
                if (file.isDirectory() && !file.getName().equals(".") && !file.getName().equals("..")) {
                    String subPath = path + "/" + file.getName();
                    stack.push(subPath);
                    filesAndDirectories.add(subPath);
                } else if (file.isFile()) {
                    filesAndDirectories.add(path + "/" + file.getName());
                }
            }
        }
        return filesAndDirectories;
    }

    protected abstract void doProcess(T data) throws Exception;
}
