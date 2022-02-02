package Assignment4.Part2;

import example.buildings.BuildingOuterClass;
import example.employees.EmployeeOuterClass;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReducerClass extends TableReducer<IntWritable, Result, ImmutableBytesWritable> {
    public ReducerClass(){
        System.out.println("in the reducer class");
    }

    @Override
    public void reduce(IntWritable key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
        int cafeteria_code =0;
        List<EmployeeOuterClass.Employee> employeeList = new ArrayList<>();

        for (Result result : values) {
            if (result.containsColumn(Bytes.toBytes("EMPLOYEE"), Bytes.toBytes("employeeDetails"))) {
                EmployeeOuterClass.Employee employee = EmployeeOuterClass.Employee.parseFrom(result.value());
                employeeList.add(employee);
            }
            if (result.containsColumn(Bytes.toBytes("BUILDING"), Bytes.toBytes("buildingDetails"))) {
                BuildingOuterClass.Building building = BuildingOuterClass.Building.parseFrom(result.value());
                cafeteria_code = Integer.parseInt(building.getCafeteriaCode());
                int building_id = Integer.parseInt(building.getBuildingCode());
            }
        }
        for (EmployeeOuterClass.Employee employee : employeeList) {
            Put put = enrichCafeteriaCode(employee, cafeteria_code);
            int employee_id = Integer.parseInt(employee.getEmployeeId());
            context.write(new ImmutableBytesWritable(Bytes.toBytes("EMPLOYEE_CAFETERIA_CODE")), put);

        }
    }

    public Put enrichCafeteriaCode(EmployeeOuterClass.Employee employee, int cafe_value){
        //employee = employee.toBuilder().setCafeteriaCode(String.valueOf(cafe_value)).build();
        String name = employee.getName();
        int employee_id = Integer.parseInt(employee.getEmployeeId());
        int building_code = Integer.parseInt(employee.getBuildingCode());
        int salary = employee.getSalary();
        String department = employee.getDepartment();
        int floor = employee.getFloorNumber().getNumber();
        //int cafeteria_code = Integer.parseInt(employee.getCafeteriaCode());


        System.out.println("employee with Cafeteria code : " + employee);

        Put put = new Put(Bytes.toBytes(employee_id));
        put.addColumn(Bytes.toBytes("employeeCafeteriaCodeDetails"), Bytes.toBytes("name"), Bytes.toBytes(name));
        put.addColumn(Bytes.toBytes("employeeCafeteriaCodeDetails"), Bytes.toBytes("building_code"), Bytes.toBytes(building_code + ""));
        put.addColumn(Bytes.toBytes("employeeCafeteriaCodeDetails"), Bytes.toBytes("employee_id"), Bytes.toBytes(employee_id + ""));
        put.addColumn(Bytes.toBytes("employeeCafeteriaCodeDetails"), Bytes.toBytes("salary"), Bytes.toBytes(salary + ""));
        put.addColumn(Bytes.toBytes("employeeCafeteriaCodeDetails"), Bytes.toBytes("department"), Bytes.toBytes(department + ""));
        put.addColumn(Bytes.toBytes("employeeCafeteriaCodeDetails"), Bytes.toBytes("floor"), Bytes.toBytes(floor + ""));
        //put.addColumn(Bytes.toBytes("employeeCafeteriaCodeDetails"), Bytes.toBytes("cafeteria_code"), Bytes.toBytes(cafeteria_code + ""));
        return put;

    }
}
