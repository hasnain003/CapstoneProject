package Assignment4.Part1;

import example.employees.EmployeeOuterClass;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.math3.analysis.function.Floor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;

public class TempEmployeeMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    public TempEmployeeMapper(){
        System.out.println("in the mapper class");
    }


    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();
    }

    ImmutableBytesWritable TABLE_NAME_TO_INSERT = new ImmutableBytesWritable(Bytes.toBytes("EMPLOYEE"));
    int row = 1;
    public void map(LongWritable key, Text value, Context context) {
        try {

            String[] values = value.toString().split(",");
            System.out.println("values" +values);
            EmployeeOuterClass.Employee employee = EmployeeOuterClass.Employee.newBuilder().setName(values[0].replaceAll("\"", ""))
                    .setEmployeeId(values[1].replaceAll("\"", ""))
                    .setBuildingCode(values[2].replaceAll("\"", ""))
                    .setSalary(Integer.parseInt(values[4].replaceAll("\"", "")))
                    .setDepartment(values[5].replaceAll("\"", ""))
                    .setFloorNumber(EmployeeOuterClass.Floor_Number.forNumber((Integer.parseInt(values[3].replaceAll("\"", "")))))
                    .build();


            System.out.println("employee row " + row);
            System.out.println("employee "+employee);

            Put p = new Put(Bytes.toBytes(row++));


            p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Name"),Bytes.toBytes(values[0].replaceAll("\"", "")));
            p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("ID"),Bytes.toBytes(values[1].replaceAll("\"", "")));
            p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Building Code"),Bytes.toBytes(values[2].replaceAll("\"", "")));
            p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Salary"),Bytes.toBytes(values[4].replaceAll("\"", "")));
            p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Department"),Bytes.toBytes(values[5].replaceAll("\"", "")));
            p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Floor Name"),Bytes.toBytes(values[3].replaceAll("\"", "")));

            context.write(TABLE_NAME_TO_INSERT, p);

        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }
}
