package Assignment4.Part1;

import example.employees.EmployeeOuterClass;
import org.apache.commons.math3.analysis.function.Floor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HBaseEmployeeBulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    public HBaseEmployeeBulkLoadMapper() {
        System.out.println("in the EMPLOYEE MAPPER class");
    }

    int row = 1;
    ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(Bytes.toBytes("EMPLOYEE"));
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        System.out.println("in the Employee map function");

        String[] values = value.toString().split(",");
        System.out.println("values array "+values.length);
        EmployeeOuterClass.Employee employee = EmployeeOuterClass.Employee.newBuilder().setName(values[0].replaceAll("\"", ""))
                .setEmployeeId(values[1].replaceAll("\"", ""))
                .setBuildingCode(values[2].replaceAll("\"", ""))
                .setSalary(Integer.parseInt(values[4].replaceAll("\"", "")))
                .setDepartment(values[5].replaceAll("\"", ""))
                .setFloorNumber(EmployeeOuterClass.Floor_Number.forNumber((Integer.parseInt(values[3].replaceAll("\"", ""))))).build();

        System.out.println("data injected to proto object : " +employee);

        Put p = new Put(Bytes.toBytes(row++));
        p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Name"),Bytes.toBytes(values[0].replaceAll("\"", "")));
        p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("ID"),Bytes.toBytes(values[1].replaceAll("\"", "")));
        p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Building Code"),Bytes.toBytes(values[2].replaceAll("\"", "")));
        p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Salary"),Bytes.toBytes(values[4].replaceAll("\"", "")));
        p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Department"),Bytes.toBytes(values[5].replaceAll("\"", "")));
        p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Floor Name"),Bytes.toBytes(values[3].replaceAll("\"", "")));
        context.write(immutableBytesWritable,p);
        System.out.println("Employee mapper class end");
    }
}
