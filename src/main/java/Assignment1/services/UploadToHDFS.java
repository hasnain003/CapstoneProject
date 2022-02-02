package Assignment1.services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.net.URI;

public class UploadToHDFS {
    public static final String HDFSNAME = "fs.defaultFS";
    public static final String HDFSPATH = "hdfs://localhost:9000/";
    public static void main(String args[]) throws Exception {
        System.out.println("Uploading to hdfs started");
        String path = "/Users/mdhasnain/FileStore/";
        String uri = "hdfs://localhost:9000/";


        File directoryPath = new File(path);
        String contents[] = directoryPath.list();
        System.out.println(contents.length);

        Configuration configuration = new Configuration();
        configuration.set(HDFSNAME,HDFSPATH);
        configuration.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
        configuration.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
        FileSystem fileSystem = FileSystem.get(new URI(HDFSPATH),configuration);
        String dir = "hdfs://localhost:9000/Users/mdhasnain/Test/";
        Path path1 = new Path(dir);
        fileSystem.mkdirs(path1);
       for(String fileName: contents) {

            System.out.println(fileName);


            fileSystem.copyFromLocalFile(new Path(path+fileName),path1);
        }
        System.out.println("Uploading to HDFS completed");
       fileSystem.close();
    }
}
