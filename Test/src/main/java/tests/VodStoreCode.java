package tests;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;



public abstract class VodStoreCode {
    private static final String BILLING_URL = "jdbc: ...";
    private static final String ETL_URL = "jdbc: ...";
    private static final DateTimeFormatter ETL_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter BILLING_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    private final ConstructorParams testConstructorParams;

    public VodStoreCode(ConstructorParams testConstructorParams) {
        this.testConstructorParams = testConstructorParams;
    }


    public static void main(String[] args) {
        // Set the logging level for the MariaDB driver to OFF
        Logger mariadbLogger = (Logger) LoggerFactory.getLogger("org.mariadb.jdbc");
        mariadbLogger.setLevel(Level.OFF);

        // Create an instance of PWStore with custom values
        ConstructorParams params = new ConstructorParams(338, 713, "2023-06-25", "2023-06-26", 5);
        VodStoreCode customStore = new VodStoreCode (params) {

        };

        // Perform the comparison
        customStore.compareDatabases();
    }

    public void compareDatabases() {
        int billingCarrierId = testConstructorParams.getBillingCarrierId();
        int etlCarrierOfferId = testConstructorParams.getEtlCarrierOfferId();
        String startDate = testConstructorParams.getStartDate();
        String finishDate = testConstructorParams.getFinishDate();
        int threshold = testConstructorParams.getThreshold();

        Map<LocalDateTime, Integer> etlSegmentsPlusTotalChargesMap = new HashMap<>();
        Map<LocalDateTime, Integer> billingSegmentsPlusCountMap = new HashMap<>();
        Set<LocalDateTime> missingSegmentsInBilling = new TreeSet<>();
        Set<LocalDateTime> missingSegmentsInETL = new TreeSet<>();
        List<String> mismatchedSegments = new LinkedList<>();
        List<Integer> mismatchedCounts = new ArrayList<>();
        List<Integer> mismatchedTotalCharges = new ArrayList<>();

        try (Connection billingConnection = DriverManager.getConnection(BILLING_URL);
             Connection etlConnection = DriverManager.getConnection(ETL_URL);
             Statement billingStatement = billingConnection.createStatement();
             Statement etlStatement = etlConnection.createStatement()) {

            // Create the billing SQL query
            String billingQuery = "SELECT \n" +
                    "    DATE_FORMAT(insertdate, '%Y-%m-%d %H:%i') AS segment,\n" +
                    "    carrier,\n" +
                    "    COUNT(id) AS count,\n" +
                    "FROM b_charges\n" +
                    "WHERE carrier = " + billingCarrierId + "\n" +
                    "    AND insertdate >= '" + startDate + "'\n" +
                    "    AND insertdate < '" + finishDate + "'\n" +
                    "GROUP BY segment, carrier;";

            // Execute the billing query
            ResultSet billingResultSet = billingStatement.executeQuery(billingQuery);

            while (billingResultSet.next()) {
                String billingSegment = billingResultSet.getString("segment");
                LocalDateTime billingDateTime = LocalDateTime.parse(billingSegment, BILLING_DATE_FORMATTER);
                int billingCount = billingResultSet.getInt("count");
                billingSegmentsPlusCountMap.put(billingDateTime, billingCount);
            }

            // Create the ETL SQL query
            String etlQuery = "SELECT \n" +
                    "    ca.carrier_offer_id,\n" +
                    "    FORMAT(ca.segment, 'yyyy-MM-dd HH:mm:ss') AS segment,\n" +
                    "    SUM(ca.total_charges) AS total_charges,\n" +
                    "FROM charge_aggregations ca\n" +
                    "WHERE ca.carrier_offer_id =" + etlCarrierOfferId + "\n" +
                    "    AND ca.segment >='" + startDate + "'\n" +
                    "    AND ca.segment < '" + finishDate + "'\n" +
                    "GROUP BY ca.carrier_offer_id, FORMAT(ca.segment, 'yyyy-MM-dd HH:mm:ss')\n" +
                    "ORDER BY segment;";

            // Execute the ETL query
            ResultSet etlResultSet = etlStatement.executeQuery(etlQuery);

            while (etlResultSet.next()) {
                String etlSegment = etlResultSet.getString("segment");
                LocalDateTime etlDateTime = LocalDateTime.parse(etlSegment, ETL_DATE_FORMATTER);
                int etlTotalCharges = etlResultSet.getInt("total_charges");
                etlSegmentsPlusTotalChargesMap.put(etlDateTime, etlTotalCharges);
            }

            // Check for missing segments in Billing
            List<LocalDateTime> missedSegmentsInEtl = new ArrayList<>(billingSegmentsPlusCountMap.keySet());
            missedSegmentsInEtl.removeAll(etlSegmentsPlusTotalChargesMap.keySet());

            missedSegmentsInEtl.forEach(segment -> missingSegmentsInETL.add(segment));
            System.out.println();
            System.out.println("Total Billing Charges: " + billingSegmentsPlusCountMap.values().stream().mapToInt(Integer::intValue).sum());
            System.out.println("Total ETL Charges: " + etlSegmentsPlusTotalChargesMap.values().stream().mapToInt(Integer::intValue).sum());
            System.out.println();

            System.out.println("Missing segments in ETL:");
            missingSegmentsInETL.stream()
                    .map(segment -> segment.format(BILLING_DATE_FORMATTER))
                    .sorted()
                    .forEach(System.out::println);
            System.out.println();

            // Check for missing segments in ETL
            List<LocalDateTime> missedSegmentsInBilling = new ArrayList<>(etlSegmentsPlusTotalChargesMap.keySet());
            missedSegmentsInBilling.removeAll(billingSegmentsPlusCountMap.keySet());

            missedSegmentsInBilling.forEach(segment -> missingSegmentsInBilling.add(segment));

            System.out.println("Missing segments in Billing:");
            missingSegmentsInBilling.stream()
                    .map(segment -> segment.format(BILLING_DATE_FORMATTER))
                    .sorted()
                    .forEach(System.out::println);
            System.out.println();

            // Check for count and total charges mismatches
            etlSegmentsPlusTotalChargesMap.keySet().removeAll(missedSegmentsInBilling);

            List<LocalDateTime> sortedSegments = new ArrayList<>(etlSegmentsPlusTotalChargesMap.keySet());
            Collections.sort(sortedSegments);

            for (LocalDateTime segment : sortedSegments) {
                int billingCount = billingSegmentsPlusCountMap.get(segment);
                int etlTotalCharges = etlSegmentsPlusTotalChargesMap.get(segment);
                if (Math.abs(billingCount - etlTotalCharges) >= threshold) {
                    mismatchedSegments.add(segment.format(BILLING_DATE_FORMATTER));
                    mismatchedCounts.add(billingCount);
                    mismatchedTotalCharges.add(etlTotalCharges);
                    System.out.println("\n");
                    System.out.println("Mismatch found at segment: " + segment.format(BILLING_DATE_FORMATTER));
                    System.out.println("Billing count: " + billingCount);
                    System.out.println("ETL total charges: " + etlTotalCharges);
                    System.out.println();

                    // Compare keys between Billing and ETL
                    Map<String, String> billingKeys = new HashMap<>();
                    Map<String, String> etlKeys = new HashMap<>();
                    // Retrieve Billing keys for this segment
                    String billingKeysQuery = "SELECT\n" +
                            "    INSERT(INSERT(INSERT(INSERT(UPPER(MD5(\n" +
                            "        CONCAT(\n" +
                            "            bc.process, '|vod-store|', CASE\n" +
                            "                WHEN bp.type = 'subscribe' THEN CONCAT('subscription|' , bp.status)\n" +
                            "                WHEN bp.type = 'renew' THEN 'renewal_attempt|retrying'\n" +
                            "                ELSE ''\n" +
                            "            END\n" +
                            "        )\n" +
                            "    )), 9, 0, '-'), 14, 0, '-'), 19, 0, '-'), 24, 0, '-') AS key_column,\n" +
                            "FROM\n" +
                            "    b_charges bc\n" +
                            "LEFT JOIN\n" +
                            "    b_processes bp ON bp.id = bc.process\n" +
                            "WHERE bc.carrier = " + billingCarrierId + "\n" +
                            "    AND bc.insertdate >= '" + segment.format(BILLING_DATE_FORMATTER) + "'\n" +
                            "    AND bc.insertdate < '" + segment.plusMinutes(1).format(BILLING_DATE_FORMATTER) + "'";

                    // Retrieve ETL keys for this segment
                    String etlKeysQuery = "SELECT *\n" +
                            "FROM charges c\n" +
                            "    LEFT JOIN user_transitions ut ON ut.[uuid] = c.[user_transition_id]\n" +
                            "    LEFT JOIN user_stories us ON us.[uuid] = ut.[user_story_id]" +
                            "WHERE us.carrier_offer_id =" + etlCarrierOfferId + "\n" +
                            "    AND c.attempted_at >= '" + segment.format(ETL_DATE_FORMATTER) + "'\n" +
                            "    AND c.attempted_at < '" + segment.plusMinutes(1).format(ETL_DATE_FORMATTER) + "'";

                    ResultSet etlKeysResultSet = etlStatement.executeQuery(etlKeysQuery);
                    ResultSet billingKeysResultSet = billingStatement.executeQuery(billingKeysQuery);
                    while (billingKeysResultSet.next()) {
                        String key = billingKeysResultSet.getString("key_column");
                        String value = billingKeysResultSet.getString("value");
                        billingKeys.put(key, value);
                    }
                    while (etlKeysResultSet.next()) {
                        String key = etlKeysResultSet.getString("user_transition_id");
                        String id = etlKeysResultSet.getString("id");
                        etlKeys.put(key, id);
                    }
                    for (String key : billingKeys.keySet()) {
                        if (!etlKeys.containsKey(key)) {
                            System.out.println("Key without a match in ETL: " + key);
                            String value = billingKeys.get(key);
                            System.out.println("Billing value: " + value);
                        }
                    }
                    // Find keys missing in Billing
                    for (String key : etlKeys.keySet()) {
                        if (!billingKeys.containsKey(key)) {
                            System.out.println("Key without a match in Billing: " + key);
                            String id = etlKeys.get(key);
                            System.out.println("ETL id: " + id);
                        }
                    }
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
