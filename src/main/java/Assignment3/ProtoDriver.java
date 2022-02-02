package Assignment3;


import example.employees.EmployeeOuterClass;
import example.employees.EmployeeOuterClass.Employee;
import example.buildings.BuildingOuterClass.Building;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ProtoDriver {
    public static final String CSV_FILE_PATH = "/Users/mdhasnain/FileStore/";
    public static void printEmployeesProto() throws IOException {
        FileReader fileReader = new FileReader(CSV_FILE_PATH+"employee.csv");
        BufferedReader br = new BufferedReader(fileReader);
        String line;
        br.readLine();
        while((line = br.readLine()) != null) {
            String[] fields = line.split(",");
            System.out.println(fields[3]);
            Employee.Builder builder = Employee.newBuilder();
            builder.setName(fields[0].replaceAll("\"", ""))
                    .setEmployeeId(fields[1].replaceAll("\"", ""))
                    .setBuildingCode(fields[2].replaceAll("\"", ""))
                    .setFloorNumber(EmployeeOuterClass.Floor_Number.forNumber(Integer.parseInt(fields[3].replaceAll("\"", ""))))
                    .setSalary(Integer.parseInt(fields[4].replaceAll("\"", "")))
                    .setDepartment(fields[5].replaceAll("\"", "")).build();
            System.out.println(builder.toString());
        }
        fileReader.close();
    }
    public static void printBuildingProto() throws IOException {
        System.out.println("started.....");
        FileReader fileReader = new FileReader(CSV_FILE_PATH+"building.csv");
        System.out.println("Opened the csv files....");
        BufferedReader br = new BufferedReader(fileReader);
        String line;
        System.out.println("Started reading the CSV files");
        br.readLine();
        while((line = br.readLine()) != null) {
            String[] fields = line.split(",");

            Building.Builder builder = Building.newBuilder();
            builder.setBuildingCode(fields[0].replaceAll("\"", ""))
                    .setTotalFloors(Integer.parseInt(fields[1].replaceAll("\"", "")))
                    .setCompaniesInTheBuilding(Integer.parseInt(fields[2].replaceAll("\"", "")))
                    .setCafeteriaCode(fields[3].replaceAll("\"", "")).build();
            System.out.println(builder.toString());
        }
        System.out.println("Finished printing proto objects");
        fileReader.close();
        System.out.println("Closed the File");
    }

    public static void main(String[] args) throws IOException {
        printBuildingProto();
        printEmployeesProto();
    }
}
