package tests;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

public class TheKioskTest {
    public static void main(String[] args) {
        Logger mariadbLogger = (Logger) LoggerFactory.getLogger("org.mariadb.jdbc");
        mariadbLogger.setLevel(Level.OFF);

        ConstructorParams ThekioskTest = new ConstructorParams(

                3020,
                343,
                "2023-07-31",
                "2023-08-01",
                1

        );

        // We get the carrier code and billing ID from the ThekioskTest object
        int carrierCode = ThekioskTest.getBillingCarrierId();
        int billingId = ThekioskTest.getEtlCarrierOfferId();

        // We get maps for the corresponding data
        Map<Integer, String> billingCarrierOfferMap = CarriersOfferData.getTheKioskBillingCarrierOfferMap();
        Map<Integer, String> etlCarrierOfferMap = CarriersOfferData.getTheKioskETLCarrierOfferMap();

        // We get the names of the carriers according to the specified codes
        String billingCarrierName = billingCarrierOfferMap.get(carrierCode);
        String etlCarrierName = etlCarrierOfferMap.get(billingId);

        // Display the names next to the corresponding fields
        System.out.println("Carrier (Billing): " + billingCarrierName);
        System.out.println("Carrier (ETL): " + etlCarrierName);

        ThekioskCode thekiosk = new ThekioskCode(ThekioskTest) {
        };
        thekiosk.compareDatabases();
    }
}