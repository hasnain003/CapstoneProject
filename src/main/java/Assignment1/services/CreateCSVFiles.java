package services;

import java.io.*;
import java.util.*;
import com.opencsv.CSVWriter;
import model.Person;
import model.PersonBuilder;

public class CreateCSVFiles {
    private static final String CSV_FILE_PATH
            = "/Users/mdhasnain/FileStore/";
    public void createKCSVFiles(int K)
    {
        String fileName = "people";
        CreateCSVFiles createCSVFiles = new CreateCSVFiles();
        PersonBuilder personBuilder = new PersonBuilder();
        for(int i = 1;i <= K;i++) {
            createCSVFiles.addDataToCSV(CSV_FILE_PATH + fileName+ i +".csv",personBuilder.getPersonsList());
        }
    }
    public  void addDataToCSV(String filePath, List<Person> personsList) {
        // first create file object for file placed at location
        // specified by filepath
        File file = new File(filePath);
        try {
            // create FileWriter object with file as parameter
            FileWriter outputfile = new FileWriter(file);

            // create CSVWriter object filewriter object as parameter
            CSVWriter writer = new CSVWriter(outputfile);

            // adding header to csv
            String[] header = {"Name", "Age", "Company", "Building_Code", "Phone_Number", "Address"};
            writer.writeNext(header);
            for(Person person:personsList) {
                writer.writeNext(new String[]{
                        person.getName(),person.getAge().toString(),person.getCompany(),person.getBuildingCode(),
                        person.getPhoneNumber(),person.getAddress()
                });
            }


            // closing writer connection
            writer.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
