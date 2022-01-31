package services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;



public class LoadToHbase {
    public static FileSystem fileSystem;
    public static String uri = "hdfs://localhost:9000/";
    public static String dir = "hdfs://localhost:9000/Users/mdhasnain/Assisgnemt/";
    public static Path[] filePaths;

    public static void LoadConfig() throws Exception {
        Configuration configuration = new Configuration();
        fileSystem = FileSystem.get(URI.create(uri),configuration);
        filePaths = readDirectoryContents();
    }

    public static Path[] readDirectoryContents() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(dir));
        Path[] paths = FileUtil.stat2Paths(fileStatuses);
        return paths;
    }

    public static void LoadOnHbase() {
        try {
            System.out.println("loading Started");
            Configuration configuration = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(configuration);

            //Verifying the existence of the table
            System.out.println("Verifying the existance of the table");
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            Table table;
            System.out.println("Check if the table already exist");
//            if(admin.tableExists(TableName.valueOf("people"))) {
//                //Table Exist so just create connection
//                System.out.println("Table already exist no need to create");
//                table = connection.getTable(TableName.valueOf("people"));
//            }
            //else {
                // Create Table
                //Instantiating table descriptor class
                System.out.println("Table creation started");
                table = createHBaseTable(admin,connection);
            //}
            for(Path path: filePaths) {
                System.out.println(path);
                addDataToHBaseTable(path,table);
            }
            table.close();
            connection.close();
            fileSystem.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static Table createHBaseTable(HBaseAdmin admin,Connection connection) {
        try {

            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("people"));
            // Adding column families to table descriptor
            // Name, Age, Company, Phone_Number,Address, Building_Code
            tableDescriptor.addFamily(new HColumnDescriptor("peoples"));

            tableDescriptor.addFamily(new HColumnDescriptor("name"));
            tableDescriptor.addFamily(new HColumnDescriptor("age"));
            tableDescriptor.addFamily(new HColumnDescriptor("company"));
            tableDescriptor.addFamily(new HColumnDescriptor("building_code"));
            tableDescriptor.addFamily(new HColumnDescriptor("phone_number"));
            tableDescriptor.addFamily(new HColumnDescriptor("address"));

            // Execute the table via admin
            System.out.println("creating table");

            admin.createTable(tableDescriptor);
            System.out.println("Done!!!");
            Table table = connection.getTable(TableName.valueOf("people"));
            return table;
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
    public static void addDataToHBaseTable(Path path,Table table) {
        try{
            int rowNum = 0;
            BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
            String line = br.readLine();
            if(path.toString().contains(".csv") == false) {
                return;
            }
            System.out.println(path.toString());
            while (line != null) {
                if(rowNum == 0) {
                    rowNum++;
                    line = br.readLine();
                    continue;
                } else {
                    String[] fields = line.split(",");
                    Put p = new Put(Bytes.toBytes("row" + rowNum));
                    System.out.println(line);
                    p.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("name"),Bytes.toBytes(fields[0]));
                    p.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("age"),Bytes.toBytes(fields[1]));
                    p.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("company"),Bytes.toBytes(fields[2]));
                    p.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("building_code"),Bytes.toBytes(fields[3]));
                    p.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("phone_number"),Bytes.toBytes(fields[4]));
                    p.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("address"),Bytes.toBytes(fields[5]));
                    table.put(p);
                    System.out.println("Inserted Row" + rowNum);
                    rowNum++;
                    line = br.readLine();
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void LoadIntoHBase() throws Exception {
        LoadConfig();
        //System.out.println("Config Done");
        LoadOnHbase();
        System.out.println("Loading Done");
    }
    public static void main(String args[]) throws Exception {
        LoadIntoHBase();
    }
}
