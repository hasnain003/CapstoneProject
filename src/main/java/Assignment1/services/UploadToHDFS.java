package services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.net.URI;

public class UploadToHDFS {
    //public static final String HDFSNAME = "fs.defaultFS";
    //public static final String HDFSPATH = "hdfs://localhost:9000/";
    public static void main(String args[]) throws Exception {
        //System.out.println("Uploading to hdfs started");
        //String path = "/Users/mdhasnain/FileStore/";
        String uri = "hdfs://localhost:9000/";


       // File directoryPath = new File(path);
        //String contents[] = directoryPath.list();
        //System.out.println(contents.length);

//        int i = 1;
//
        for(int i = 1;i<=100;i++) {
            String localpath="/Users/mdhasnain/FileStore/people"+Integer.toString(i)+".csv";
            String dir = "hdfs://localhost:9000/Users/mdhasnain/Assisgnemt/";
            //System.out.println(fileName);
            Configuration configuration = new Configuration();
            FileSystem fileSystem = FileSystem.get(URI.create(uri),configuration);
            fileSystem.copyFromLocalFile(new Path(localpath),new Path(dir));
        }
//       for(String fileName: contents) {
//           String dir = "hdfs://localhost:9000/Users/mdhasnain/Assisgnemt/";
//           //System.out.println(fileName);
//            Configuration configuration = new Configuration();
//            FileSystem fileSystem = FileSystem.get(new URI(HDFSPATH),configuration);
//            fileSystem.copyFromLocalFile(new Path(path+fileName),new Path(dir));
//        }
        System.out.println("Uploading to HDFS completed");
        //return fileSystem;
    }
}
