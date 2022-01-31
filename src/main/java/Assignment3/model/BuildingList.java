package Assignment3.model;

import com.github.javafaker.Faker;

import java.util.ArrayList;
import java.util.List;

public class BuildingList {
    public static Building getBuilding() {
        Building building = new Building();
        Faker faker = new Faker();
        building.setBuildingCode(String.valueOf(faker.number().numberBetween(1,10000)));
        building.setCafeteriaCode(String.valueOf(faker.number().numberBetween(1,10)));
        building.setCompaniesInBuilding(faker.number().numberBetween(1,80));
        building.setTotalFloors(faker.number().numberBetween(1,15));
        return building;
    }

    public static List<Building> getBuildingList() {
        List<Building> buildingList = new ArrayList<>();
        for(int i = 1;i<=10;i++) {
            buildingList.add(getBuilding());
        }
        return buildingList;
    }
}
