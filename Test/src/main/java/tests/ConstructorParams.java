package tests;

public class ConstructorParams {



    private int billingCarrierId;
    private int etlCarrierOfferId;
    private String startDate;
    private String finishDate;
    private int threshold;

    private String billingType;
    private String etlType;

    public ConstructorParams(int billingCarrierId, int etlCarrierOfferId, String startDate, String finishDate, int threshold, String billingType, String etlType) {
        this.billingCarrierId = billingCarrierId;
        this.etlCarrierOfferId = etlCarrierOfferId;
        this.startDate = startDate;
        this.finishDate = finishDate;
        this.threshold = threshold;
        this.billingType = billingType;
        this.etlType = etlType;
    }

    public ConstructorParams(int billingCarrierId, int etlCarrierOfferId, String startDate, String finishDate, int threshold) {
        this.billingCarrierId = billingCarrierId;
        this.etlCarrierOfferId = etlCarrierOfferId;
        this.startDate = startDate;
        this.finishDate = finishDate;
        this.threshold = threshold;
    }
    public ConstructorParams(int billingCarrierId, int etlCarrierOfferId, String startDate, String finishDate) {
        this.billingCarrierId = billingCarrierId;
        this.etlCarrierOfferId = etlCarrierOfferId;
        this.startDate = startDate;
        this.finishDate = finishDate;
    }

    public int getBillingCarrierId() {
        return billingCarrierId;
    }

    public void setBillingCarrierId(int billingCarrierId) {
        this.billingCarrierId = billingCarrierId;
    }

    public int getEtlCarrierOfferId() {
        return etlCarrierOfferId;
    }

    public void setEtlCarrierOfferId(int etlCarrierOfferId) {
        this.etlCarrierOfferId = etlCarrierOfferId;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getFinishDate() {
        return finishDate;
    }

    public void setFinishDate(String finishDate) {
        this.finishDate = finishDate;
    }

    public int getThreshold() {
        return threshold;
    }

    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }
    public String getBillingType() {
        return billingType;
    }

    public void setBillingType(String billingType) {
        this.billingType = billingType;
    }

    public String getEtlType() {
        return etlType;
    }

    public void setEtlType(String etlType) {
        this.etlType = etlType;
    }

}