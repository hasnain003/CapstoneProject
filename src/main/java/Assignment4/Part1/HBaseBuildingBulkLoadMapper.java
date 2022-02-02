package Assignment4.Part1;

import example.buildings.BuildingOuterClass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HBaseBuildingBulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    public HBaseBuildingBulkLoadMapper() {
        System.out.println("in the BUILDING MAPPER class");
    }

    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();
    }

    ImmutableBytesWritable TABLE_NAME_TO_INSERT = new ImmutableBytesWritable(Bytes.toBytes("BUILDING"));
    int row = 1;

    public void map(LongWritable key, Text value, Context context) {
        try {

            String[] values = value.toString().split(",");
            System.out.println("values" + values);


            BuildingOuterClass.Building building = BuildingOuterClass.Building.newBuilder()
                    .setBuildingCode(values[0].replaceAll("\"", ""))
                    .setTotalFloors(Integer.parseInt(values[1].replaceAll("\"", "")))
                    .setCompaniesInTheBuilding(Integer.parseInt(values[2].replaceAll("\"", "")))
                    .setCafeteriaCode(values[3].replaceAll("\"", "")).build();


            System.out.println("building row " + row);
            System.out.println("building " + building);

            Put p = new Put(Bytes.toBytes(row++));


            p.addColumn(Bytes.toBytes("buildingDetails"), Bytes.toBytes("Building Code"), Bytes.toBytes(values[0]));
            p.addColumn(Bytes.toBytes("buildingDetails"), Bytes.toBytes("Total Floor"), Bytes.toBytes(values[1]));
            p.addColumn(Bytes.toBytes("buildingDetails"), Bytes.toBytes("Number of companies"), Bytes.toBytes(values[2]));
            p.addColumn(Bytes.toBytes("buildingDetails"), Bytes.toBytes("Cafeteria Code"), Bytes.toBytes(values[3]));
            //  p.addColumn(Bytes.toBytes("employeeDetails"), Bytes.toBytes("Department"), Bytes.toBytes(values[4]));
            //  p.addColumn(Bytes.toBytes("employeeDetails"), Bytes.toBytes("Floor Name"), Bytes.toBytes(values[5]));

            context.write(TABLE_NAME_TO_INSERT, p);

        } catch (Exception exception) {
            exception.printStackTrace();
        }

    }
}
