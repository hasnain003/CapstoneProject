package Assignment4.Part2;


import example.buildings.BuildingOuterClass;
import example.employees.EmployeeOuterClass;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Arrays;

public class MapperClass extends TableMapper<IntWritable, Result> {
    public MapperClass(){
        System.out.println("in the mapper Class");
    }


    @Override
    public void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
        TableSplit tableSplit = (TableSplit) context.getInputSplit();
        byte[] tablename = tableSplit.getTableName();
        if(Arrays.equals(tablename, Bytes.toBytes("EMPLOYEE"))){
            EmployeeOuterClass.Employee employee = EmployeeOuterClass.Employee.parseFrom(result.value());
            System.out.println("Employee : " + employee);
            int building_code = Integer.parseInt(employee.getBuildingCode());
            context.write(new IntWritable(building_code),result);
        }
        else{
            BuildingOuterClass.Building building = BuildingOuterClass.Building.parseFrom(result.value());
            System.out.println("Building : " + building);
            int building_code= Integer.parseInt(building.getBuildingCode());
            context.write(new IntWritable(building_code),result);
        }
    }
}
