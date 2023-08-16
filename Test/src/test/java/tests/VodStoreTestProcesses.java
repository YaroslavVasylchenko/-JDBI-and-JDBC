package tests;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

public class VodStoreTestProcesses {
    public static void main(String[] args) {
        Logger mariadbLogger = (Logger) LoggerFactory.getLogger("org.mariadb.jdbc");
        mariadbLogger.setLevel(Level.OFF);

        //          TYPE_PROCESSES_FOR_BILLING       TYPE_PROCESSES_FOR_ETL

        //          "subscribe"                      "subscription"
        //          "unsubscribe"                    "unsubscription"
        //          "renew"                          "renewal_finalized"
        //          "ident"                          "identification"

        ConstructorParams VodStoreTestProcesses = new ConstructorParams(
                338,
                713,
                "2023-07-19 10:00",
                "2023-07-19 11:00",
                1,
                "subscribe",
                "subscription"
        );


        // We get the carrier code and billing ID from the ThekioskTest object
        int carrierCode = VodStoreTestProcesses.getBillingCarrierId();
        int billingId = VodStoreTestProcesses.getEtlCarrierOfferId();

        // We get maps for the corresponding data
        Map<Integer, String> billingCarrierOfferMap = CarriersOfferData.getVodBillingCarrierOfferMap();
        Map<Integer, String> etlCarrierOfferMap = CarriersOfferData.getVodETLCarrierOfferMap();

        // We get the names of the carriers according to the specified codes
        String billingCarrierName = billingCarrierOfferMap.get(carrierCode);
        String etlCarrierName = etlCarrierOfferMap.get(billingId);

        // Display the names next to the corresponding fields
        System.out.println("Carrier (Billing): " + billingCarrierName);
        System.out.println("Carrier (ETL): " + etlCarrierName);


        VodStoreProcessesCode vodStore = new VodStoreProcessesCode(VodStoreTestProcesses) {
        };
        vodStore.compareDatabases();


    }
}
