package Assignment3.model;

public class Building {
    private String buildingCode;
    private Integer totalFloors;
    private Integer companiesInBuilding;
    private String cafeteriaCode;

    public String getBuildingCode() {
        return buildingCode;
    }

    public void setBuildingCode(String buildingCode) {
        this.buildingCode = buildingCode;
    }

    public Integer getTotalFloors() {
        return totalFloors;
    }

    public void setTotalFloors(Integer totalFloors) {
        this.totalFloors = totalFloors;
    }

    public Integer getCompaniesInBuilding() {
        return companiesInBuilding;
    }

    public void setCompaniesInBuilding(Integer companiesInBuilding) {
        this.companiesInBuilding = companiesInBuilding;
    }

    public String getCafeteriaCode() {
        return cafeteriaCode;
    }

    public void setCafeteriaCode(String cafeteriaCode) {
        this.cafeteriaCode = cafeteriaCode;
    }
}
