package Assignment1;

import Assignment1.services.CreateCSVFiles;
import Assignment1.services.LoadToHbase;
import Assignment1.services.UploadToHDFS;

public class Driver {

    public static void main(String[] args) throws Exception {

        // Created 100 CSV Files and stored them locally
        CreateCSVFiles createCSVFiles = new CreateCSVFiles();
        createCSVFiles.createKCSVFiles(100);

        // Now we upload this to HDFS
        UploadToHDFS uploadToHDFS = new UploadToHDFS();
        //FileSystem fileSystem = uploadToHDFS.fileUpload();
        //UploadToHDFS.main();

        //Thread.sleep(1000);

        // loading all these files to HBase Table
        LoadToHbase loadToHbase = new LoadToHbase();

    }
}
