package Assignment4.Part1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class Driver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        System.out.println("configuration = " +conf);
        FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:9000/"), conf);
        System.out.println("File System = " + hdfs);
        hdfs.delete(new Path("hdfs://localhost:9000/user"),true);
        //   hdfs.delete(new Path("hdfs://localhost:9000/Employee.csv"),true);
        hdfs.delete(new Path("hdfs://localhost:9000/BuildingResult"),true);
    }
}
