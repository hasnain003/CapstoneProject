package Assignment4.Part2;

import example.results.ResultEmp;
import example.results.ResultEmp.Result;
import Assignment4.Part1.HBaseConnect;
import example.employees.EmployeeOuterClass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class DriverClass {
    public static final String CSV_FILE_PATH = "/Users/mdhasnain/FileStore/";
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        List<Scan> scans = new ArrayList<>();
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.setAttribute("scan.attributes.table.name", Bytes.toBytes("EMPLOYEE"));
        scans.add(scan);
        Scan scan1 = new Scan();
        scan1.setCaching(500);
        scan1.setCacheBlocks(false);
        scan1.setAttribute("scan.attributes.table.name", Bytes.toBytes("BUILDING"));
        scans.add(scan1);

        Configuration configuration = HBaseConfiguration.create();
        System.out.println("configuration "+configuration);
        Job job = Job.getInstance(configuration);
        job.setJobName("MRTableReadWrite");
        job.setJarByClass(DriverClass.class);

        TableMapReduceUtil.initTableMapperJob(scans,MapperClass.class, IntWritable.class, Result.class,job);
        TableMapReduceUtil.initTableReducerJob("EMPLOYEE_CAFETERIA_CODE",ReducerClass.class,job);

        Path output = new Path("hdfs://localhost:9000/assignment4b");

        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000/assignment4b"),configuration);
        if(fs.exists(output)){
            fs.delete(output,true);
        }
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job,new Path("hdfs://localhost:9000/assignment4b"));
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        if (job.isSuccessful()) {
            System.out.println("Cafeteria code added to employee table");
        }
        joinEmployeesAndBuilding();
    }
    public static void joinEmployeesAndBuilding() throws IOException {
        ArrayList<String[]> employees = new ArrayList<>();
        ArrayList<String[]> buildings = new ArrayList<>();
        employees = getInput(CSV_FILE_PATH+"employee.csv");
        buildings = getInput(CSV_FILE_PATH + "building.csv");
        for(String[] employee : employees) {
            for(String[] building:buildings) {
                if(employee[2].equals(building[0])) {
                    Result.Builder builder = Result.newBuilder();
                    builder.setName(employee[0].replaceAll("\"", ""))
                            .setEmployeeId(employee[1].replaceAll("\"", ""))
                            .setBuildingCode(employee[2].replaceAll("\"", ""))
                            .setFloorNumber(ResultEmp.Floor_Number.forNumber(Integer.parseInt(employee[3].replaceAll("\"", ""))))
                            .setSalary(Integer.parseInt(employee[4].replaceAll("\"", "")))
                            .setDepartment(employee[5].replaceAll("\"", ""))
                            .setCafeteriaCode(building[3].replaceAll("\"", "")).build();
                    System.out.println(builder.toString());
                }
            }
        }
    }

    public static ArrayList<String[]> getInput(String filepath) throws IOException {
        ArrayList<String[]> tmp = new ArrayList<>();
        FileReader fileReader = new FileReader(filepath);
        BufferedReader br = new BufferedReader(fileReader);
        String line;
        br.readLine();
        while((line = br.readLine()) != null) {
            String[] fields = line.split(",");
            tmp.add(fields);
        }
        fileReader.close();
        return tmp;
    }

}
