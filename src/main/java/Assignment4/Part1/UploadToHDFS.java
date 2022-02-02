package Assignment4.Part1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class UploadToHDFS {
    public static final String HDFSNAME = "fs.defaultFS";
    public static final String HDFSPATH = "hdfs://localhost:9000/";
    public static void uploadToHDFS(String path,String targetPath) throws IOException, URISyntaxException {
        Configuration configuration = new Configuration();
        configuration.set(HDFSNAME,HDFSPATH);
        configuration.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
        configuration.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
        FileSystem fileSystem = FileSystem.get(new URI(HDFSPATH),configuration);
        //String dir = "hdfs://localhost:9000/Employee.csv";
        Path path1 = new Path(targetPath);
        fileSystem.copyFromLocalFile(new Path(path),path1);
        fileSystem.close();
    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        String path = "/Users/mdhasnain/FileStore/";
        String targetPath = "hdfs://localhost:9000/";
         uploadToHDFS((path + "employee.csv"),targetPath+"Employee.csv");
         uploadToHDFS(path + "building.csv",targetPath+"Building.csv");
    }
}
