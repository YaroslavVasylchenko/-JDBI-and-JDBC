package tests;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

public class PWStoreTest {
    public static void main(String[] args) {
        Logger mariadbLogger = (Logger) LoggerFactory.getLogger("org.mariadb.jdbc");
        mariadbLogger.setLevel(Level.OFF);

        ConstructorParams PWStoreTest = new ConstructorParams(

                338,
                1,
                "2023-08-05 ",
                "2023-08-06 ",
                1

        );

        // We get the carrier code and billing ID from the PWStoreTest object
        int carrierCode = PWStoreTest.getBillingCarrierId();
        int billingId = PWStoreTest.getEtlCarrierOfferId();

        // We get maps for the corresponding data
        Map<Integer, String> billingCarrierOfferMap = CarriersOfferData.getPWStoreBillingCarrierOfferMap();
        Map<Integer, String> etlCarrierOfferMap = CarriersOfferData.getPWStoreETLCarrierOfferMap();

        // We get the names of the carriers according to the specified codes
        String billingCarrierName = billingCarrierOfferMap.get(carrierCode);
        String etlCarrierName = etlCarrierOfferMap.get(billingId);

        // Display the names next to the corresponding fields
        System.out.println("Carrier (Billing): " + billingCarrierName);
        System.out.println("Carrier (ETL): " + etlCarrierName);

        PWStoreCode pwStore = new PWStoreCode(PWStoreTest) {
        };
        pwStore.compareDatabases();
    }
}








