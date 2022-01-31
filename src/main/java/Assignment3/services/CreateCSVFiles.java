package Assignment3.services;

import Assignment1.model.Person;
import Assignment3.model.Building;
import Assignment3.model.BuildingList;
import Assignment3.model.Employee;
import Assignment3.model.EmployeeList;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class CreateCSVFiles {
    private static final String CSV_FILE_PATH
            = "/Users/mdhasnain/FileStore/";
    public static void createEmployeeCSVFile() {
        File file = new File(CSV_FILE_PATH+"employee.csv");
        try {
            // create FileWriter object with file as parameter
            System.out.println("started the creation of EmployeeCSV files");
            FileWriter outputfile = new FileWriter(file);

            // create CSVWriter object filewriter object as parameter
            CSVWriter writer = new CSVWriter(outputfile);

            // adding header to csv
            String[] header = {"Name", "EmployeeID", "Building_Code", "Floor_Number", "Salary", "Department"};
            writer.writeNext(header);
            List<Employee> employeeList = EmployeeList.getEmployeeList();
            for(Employee employee:employeeList) {
                writer.writeNext(new String[]{
                        employee.getName(),employee.getEmployee_id(),employee.getBuilding_code(),employee.getFloor_number().toString(),
                        employee.getSalary().toString(),employee.getDepartment()
                });
            }

            // closing writer connection
            System.out.println("EmployeeCSV Successfully Created");
            writer.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    public static  void createBuildingCSVFile() {
        File file = new File(CSV_FILE_PATH+"building.csv");
        try {

            System.out.println("Building CSV creation started");

            FileWriter outputfile = new FileWriter(file);

            CSVWriter writer = new CSVWriter(outputfile);

            String[] header = {"BuildingCode","Total_Floor","Companies_In_Building","CafeteriaCode"};
            writer.writeNext(header);
            List<Building> buildingList = BuildingList.getBuildingList();
            for(Building building:buildingList) {
                writer.writeNext(new String[]{
                        building.getBuildingCode(),building.getTotalFloors().toString(),building.getCompaniesInBuilding().toString(),
                        building.getCafeteriaCode()
                });
            }
            System.out.println("Building CSV files creation done successfully");
            writer.close();
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }
    public static void main(String[] args) {
        System.out.println("Started");
        createEmployeeCSVFile();
        createBuildingCSVFile();
    }
}
