import java.io.*;
import java.util.*;
import com.opencsv.CSVWriter;

public class CreateCSVFiles {
    private static final String CSV_FILE_PATH
            = "/Users/mdhasnain/FileStore/";
    public static void main(String[] args)
    {
        String fileName = "people";
        CreateCSVFiles createCSVFiles = new CreateCSVFiles();
        for(int i = 1;i <= 100;i++) {
            createCSVFiles.addDataToCSV(CSV_FILE_PATH + fileName +".csv");
        }
    }
    public  void addDataToCSV(String filePath)
    {
        // first create file object for file placed at location
        // specified by filepath
        File file = new File(filePath);
        try {
            // create FileWriter object with file as parameter
            FileWriter outputfile = new FileWriter(file);

            // create CSVWriter object filewriter object as parameter
            CSVWriter writer = new CSVWriter(outputfile);

            // adding header to csv
            String[] header = { "Name", "Age", "Company","Building_Code","Phone_Number","Address"};
            writer.writeNext(header);
            for(int i = 0;i<5;i++) {
                // add data to csv

                String[] data1 = getInputForFile();
                writer.writeNext(data1);
                String[] data2 = { "Suraj", "10", "Sigmoid Analytics","101","9821213462","Park Circus"};
                writer.writeNext(data2);
            }

            // closing writer connection
            writer.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    public String[] getInputForFile() {
        Scanner sc = new Scanner(System.in);
        String[] data = new String[6];
        System.out.println("Enter Name:");
        data[0] = sc.nextLine();
        System.out.println("Enter Age:");
        data[1] = sc.nextLine();
        System.out.println("Enter Company:");
        data[2] = sc.nextLine();
        System.out.println("Enter Building_Number:");
        data[3] = sc.nextLine();
        System.out.println("Enter Phone_Number:");
        data[4] = sc.nextLine();
        System.out.println("Enter Address:");
        data[5] = sc.nextLine();
        return data;
    }
}
