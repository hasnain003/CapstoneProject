package Assignment1.services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;


public class CreateDirectory {
    private static final String uri = "hdfs://localhost:9000/Users/mdhasnain/";
    private static final String directory = uri +"Test";
    static final Configuration configuration = new Configuration();

    public static void createDirectory() throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(uri), configuration);
        boolean isCreated  = fileSystem.mkdirs(new Path(directory));

        if(isCreated) {
            System.out.println("Directory created");
        } else {
            System.out.println("Directory creation failed");
        }
    }
    public static void main(String args[]) throws IOException {
        createDirectory();
    }
}
