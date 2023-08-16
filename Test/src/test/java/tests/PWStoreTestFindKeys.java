package tests;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

public class PWStoreTestFindKeys {

    public static void main(String[] args) {
        Logger mariadbLogger = (Logger) LoggerFactory.getLogger("org.mariadb.jdbc");
        mariadbLogger.setLevel(Level.OFF);

        ConstructorParams PWStoreTestToFindKeys = new ConstructorParams(
                338,
                1,
                "2023-08-05 08:00",
                "2023-08-05 10:00"
        );

        PWStoreTestFindKeys pwStoreTestToFindKeys = new PWStoreTestFindKeys(PWStoreTestToFindKeys);
        pwStoreTestToFindKeys.run();
    }

    private final ConstructorParams testConstructorParams;

    public PWStoreTestFindKeys(ConstructorParams testConstructorParams) {
        this.testConstructorParams = testConstructorParams;
    }

    public void run() {
        int carrierCode = testConstructorParams.getBillingCarrierId();
        int billingId = testConstructorParams.getEtlCarrierOfferId();

        Map<Integer, String> billingCarrierOfferMap = CarriersOfferData.getPWStoreBillingCarrierOfferMap();
        Map<Integer, String> etlCarrierOfferMap = CarriersOfferData.getPWStoreETLCarrierOfferMap();

        String billingCarrierName = billingCarrierOfferMap.get(carrierCode);
        String etlCarrierName = etlCarrierOfferMap.get(billingId);

        System.out.println("Carrier (Billing): " + billingCarrierName);
        System.out.println("Carrier (ETL): " + etlCarrierName);

        PWStoreCodeToFindKeys pwStore = new PWStoreCodeToFindKeys(testConstructorParams);
        pwStore.compareDatabases();
    }
}