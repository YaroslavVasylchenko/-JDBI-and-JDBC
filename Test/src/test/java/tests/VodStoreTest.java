package tests;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class VodStoreTest {
    public static void main(String[] args) {
        Logger mariadbLogger = (Logger) LoggerFactory.getLogger("org.mariadb.jdbc");
        mariadbLogger.setLevel(Level.OFF);

        ConstructorParams VodStoreTest = new ConstructorParams(

                338,
                713,
                "2023-06-26",
                "2023-06-27",
                5

        );

        // We get the carrier code and billing ID from the PWStoreTest object
        int carrierCode = VodStoreTest.getBillingCarrierId();
        int billingId = VodStoreTest.getEtlCarrierOfferId();

        // We get maps for the corresponding data
        Map<Integer, String> billingCarrierOfferMap = CarriersOfferData.getVodBillingCarrierOfferMap();
        Map<Integer, String> etlCarrierOfferMap = CarriersOfferData.getVodETLCarrierOfferMap();

        // We get the names of the carriers according to the specified codes
        String billingCarrierName = billingCarrierOfferMap.get(carrierCode);
        String etlCarrierName = etlCarrierOfferMap.get(billingId);

        // Display the names next to the corresponding fields
        System.out.println("Carrier (Billing): " + billingCarrierName);
        System.out.println("Carrier (ETL): " + etlCarrierName);

        VodStoreCode vodStore = new VodStoreCode(VodStoreTest) {
        };
        vodStore.compareDatabases();
    }
}