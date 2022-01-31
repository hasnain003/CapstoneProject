package model;

import com.github.javafaker.Faker;

import java.util.ArrayList;
import java.util.List;

public class PersonBuilder {
    private  Person getPerson() {
        Faker faker = new Faker();
        String name = faker.name().fullName();
        Integer age = faker.number().numberBetween(19,52);
        String address = faker.address().fullAddress();
        String company = faker.company().toString();
        String buildingCode = faker.code().toString();
        String phoneNumber = faker.phoneNumber().phoneNumber();

        Person person = new Person();
        person.setName(name);
        person.setAddress(address);
        person.setAge(age);
        person.setCompany(company);
        person.setBuildingCode(buildingCode);
        person.setPhoneNumber(phoneNumber);
        return person;
    }
    public  List<Person> getPersonsList() {
        List<Person> personList = new ArrayList<>();
        int range = (int)(Math.random() * 10) + 5;
        for(int i = 0;i < range;i++) {
            personList.add(this.getPerson());
        }
        return personList;
    }
}
