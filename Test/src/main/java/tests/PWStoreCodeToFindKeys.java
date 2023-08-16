package tests;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class PWStoreCodeToFindKeys {
    private static final String BILLING_URL ="jdbc: ...";
    private static final String ETL_URL = "jdbc: ...";
    private final ConstructorParams testConstructorParams;

    public PWStoreCodeToFindKeys(ConstructorParams testConstructorParams) {
        this.testConstructorParams = testConstructorParams;
    }

    public static void main(String[] args) {
        Logger mariadbLogger = (Logger) LoggerFactory.getLogger("org.mariadb.jdbc");
        mariadbLogger.setLevel(Level.OFF);

        ConstructorParams params = new ConstructorParams(338, 1, "2023-08-03 09:00:00", "2023-08-03 09:30:00");
        PWStoreCodeToFindKeys pwStoreCode = new PWStoreCodeToFindKeys(params);
        pwStoreCode.compareDatabases();
    }

    public void compareDatabases() {
        Connection billingConnection = null;
        Connection etlConnection = null;
        Statement billingStatement = null;
        Statement etlStatement = null;
        ResultSet billingResultSet = null;
        ResultSet etlResultSet = null;

        Map<String, Integer> KeyCounts = new HashMap<>();
        Map<String, String> lostChargesDetails = new HashMap<>();
        Map<String, String> billingValues = new HashMap<>();
        Map<String, String> etlIds = new HashMap<>();

        int billingCarrierId = testConstructorParams.getBillingCarrierId();
        int etlCarrierOfferId = testConstructorParams.getEtlCarrierOfferId();
        String startTime = testConstructorParams.getStartDate();
        String endTime = testConstructorParams.getFinishDate();

        try {
            billingConnection = DriverManager.getConnection(BILLING_URL);
            etlConnection = DriverManager.getConnection(ETL_URL);
            long startTime1 = System.currentTimeMillis();


            String billingQuery = "SELECT REPLACE(UPPER(MD5(\n" +
                    "    CONCAT(\n" +
                    "        bc.process, '|pw_store|', CASE\n" +
                    "            WHEN bp.type = 'subscribe' THEN CONCAT('subscription|' , bp.status)\n" +
                    "            WHEN bp.type = 'renew' THEN 'renewal_attempt|retrying'\n" +
                    "            ELSE ''\n" +
                    "        END\n" +
                    ")\n" +
                    ")), '-', '') AS key_column,\n" +
                    "    bp.type,\n" +
                    "FROM b_charges bc\n" +
                    "    LEFT JOIN b_processes bp ON bp.id = bc.process\n" +
                    "WHERE bc.carrier = " + billingCarrierId + "\n" +
                    "    AND bc.insertdate >= '" + startTime + "'\n" +
                    "    AND bc.insertdate < '" + endTime + "';";

            String etlQuery = "SELECT REPLACE(user_transition_id, '-', '') AS key_column_etl, c.*, attempted_at\n" +
                    "FROM charges c\n" +
                    "    LEFT JOIN user_stories us ON us.[uuid] = ut.[user_story_id]\n" +
                    "WHERE us.carrier_offer_id = " + etlCarrierOfferId + "\n" +
                    "    AND c.attempted_at >= '" + startTime + "'\n" +
                    "    AND c.attempted_at < '" + endTime + "';";

            billingStatement = billingConnection.createStatement();
            billingResultSet = billingStatement.executeQuery(billingQuery);

            etlStatement = etlConnection.createStatement();
            etlResultSet = etlStatement.executeQuery(etlQuery);

            long elapsedTimeMillis = System.currentTimeMillis() - startTime1;
            double elapsedTimeSeconds = (double) elapsedTimeMillis / 1000.0;


            System.out.println("Execution time: " + elapsedTimeSeconds + " sec");

            while (etlResultSet.next()) {
                String etlKey = etlResultSet.getString("key_column_etl");
                String etlValue = etlResultSet.getString("id");
                KeyCounts.put(etlKey, KeyCounts.getOrDefault(etlKey, 0) + 1);
                etlIds.put(etlKey, etlValue);
            }

            int lostChargeCount = 0;

            while (billingResultSet.next()) {
                String billingKey = billingResultSet.getString("key_column");
                String insertDate = billingResultSet.getString("insertdate");
                String modDate = billingResultSet.getString("moddate");
                int etlCount = KeyCounts.getOrDefault(billingKey, 0);
                KeyCounts.put(billingKey, etlCount - 1);

                if (etlCount <= 0) {
                    lostChargeCount++;
                    lostChargesDetails.put(billingKey, (modDate != null ? modDate : insertDate));
                    String billingValue = billingResultSet.getString("value");
                    if (billingValue != null) {
                        billingValues.put(billingKey, billingValue);
                    }
                }
            }

            System.out.println("Total Lost Charges: " + lostChargeCount);
            System.out.println();

            for (Map.Entry<String, String> entry : lostChargesDetails.entrySet()) {
                System.out.println("Time: " + entry.getValue());
                System.out.println("Key without a match: " + entry.getKey());

                String billingValue = billingValues.get(entry.getKey());
                if (billingValue != null) {
                    System.out.println("Billing Value: " + billingValue);
                } else {
                    String etlId = etlIds.get(entry.getKey());
                    if (etlId != null) {
                        System.out.println("ETL ID: " + etlId);
                    }
                }




                System.out.println();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (billingResultSet != null) {
                    billingResultSet.close();
                }
                if (etlResultSet != null) {
                    etlResultSet.close();
                }
                if (billingStatement != null) {
                    billingStatement.close();
                }
                if (etlStatement != null) {
                    etlStatement.close();
                }
                if (billingConnection != null) {
                    billingConnection.close();
                }
                if (etlConnection != null) {
                    etlConnection.close();
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
