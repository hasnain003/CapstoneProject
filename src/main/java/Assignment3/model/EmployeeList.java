package Assignment3.model;

import com.github.javafaker.Faker;

import java.util.ArrayList;
import java.util.List;

public class EmployeeList {

    public static Employee getEmployee() {
        Employee employee = new Employee();
        Faker faker = new Faker();
        employee.setName(faker.name().fullName());
        employee.setEmployee_id(String.valueOf(faker.number().numberBetween(10,10000)));
        employee.setBuilding_code(String.valueOf(faker.number().numberBetween(1,100)));
        employee.setFloor_number(faker.number().numberBetween(1,11));
        employee.setSalary(faker.number().numberBetween(500000,90000000));
        employee.setDepartment(faker.company().industry());
        return employee;
    }

    public static List<Employee> getEmployeeList() {
        List<Employee> employeeList = new ArrayList<>();
        for(int i = 1;i<=10;i++) {
            employeeList.add(getEmployee());
        }
        return employeeList;
    }

}
