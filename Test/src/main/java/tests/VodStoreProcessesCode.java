package tests;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

public class VodStoreProcessesCode {
    private static final String BILLING_URL = "jdbc: ...";
    private static final String ETL_URL = "jdbc: ...";
    private static final DateTimeFormatter ETL_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter BILLING_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    private final ConstructorParams testConstructorParams;

    public VodStoreProcessesCode(ConstructorParams testConstructorParams) {
        this.testConstructorParams = testConstructorParams;
    }

    public static void main(String[] args) {
        // Set the logging level for the MariaDB driver to OFF
        Logger mariadbLogger = (Logger) LoggerFactory.getLogger("org.mariadb.jdbc");
        mariadbLogger.setLevel(Level.OFF);

        // Create an instance of VodStore with custom values
        ConstructorParams params = new ConstructorParams(338, 713, "2023-07-19 ", "2023-07-20", 1,"subscribe","subscription");

        VodStoreProcessesCode customKiosk = new VodStoreProcessesCode(params);
        customKiosk.compareDatabases();

    }

    public void compareDatabases() {
        int billingCarrierId = testConstructorParams.getBillingCarrierId();
        int etlCarrierOfferId = testConstructorParams.getEtlCarrierOfferId();
        String startDate = testConstructorParams.getStartDate();
        String finishDate = testConstructorParams.getFinishDate();
        int threshold = testConstructorParams.getThreshold();
        String billingType = testConstructorParams.getBillingType();
        String etlType = testConstructorParams.getEtlType();

        Map<LocalDateTime, Integer> billingSegmentsPlusCountMap = new HashMap<>();
        Map<LocalDateTime, Integer> etlSegmentsPlusTotalTransitionsMap = new HashMap<>();
        Set<LocalDateTime> missingSegmentsInBilling = new TreeSet<>();
        Set<LocalDateTime> missingSegmentsInETL = new TreeSet<>();
        List<String> mismatchedSegments = new LinkedList<>();
        List<Integer> mismatchedCounts = new ArrayList<>();
        List<Integer> mismatchedTotalTransitions = new ArrayList<>();

        try (Connection billingConnection = DriverManager.getConnection(BILLING_URL);
             Connection etlConnection = DriverManager.getConnection(ETL_URL);
             Statement billingStatement = billingConnection.createStatement();
             Statement etlStatement = etlConnection.createStatement()) {

            // Create the billing SQL query
            String billingQuery = "SELECT DATE_FORMAT(moddate, '%Y-%m-%d %H:%i') AS segment, COUNT(id) AS COUNT " +
                    "FROM b_processes bp " +
                    "WHERE bp.carrier = " + billingCarrierId + " " +
                    "AND moddate >= '" + startDate + "' " +
                    "AND moddate < '" + finishDate + "' " +
                    "AND type = '" + billingType + "' " +
                    "AND status IN ('successful', 'failed') " +
                    "GROUP BY DATE_FORMAT(moddate, '%Y-%m-%d %H:%i');";

            // Execute the billing query
            ResultSet billingResultSet = billingStatement.executeQuery(billingQuery);

            while (billingResultSet.next()) {
                String billingSegment = billingResultSet.getString("segment");
                LocalDateTime billingDateTime = LocalDateTime.parse(billingSegment, BILLING_DATE_FORMATTER);
                int billingCount = billingResultSet.getInt("COUNT");
                billingSegmentsPlusCountMap.put(billingDateTime, billingCount);
            }

            // Create the ETL SQL query
            String etlQuery = "SELECT\n" +
                    "  FORMAT(segment, 'yyyy-MM-dd HH:mm:ss') AS segment,\n" +
                    "FROM user_transition_aggregations uta\n" +
                    "WHERE carrier_offer_id = " + etlCarrierOfferId + " " +
                    "  AND segment >=  '" + startDate + "' " +
                    "  AND segment < '" + finishDate + "' " +
                    "  AND [type] = '" + etlType + "' " +
                    "GROUP BY FORMAT(segment, 'yyyy-MM-dd HH:mm:ss');";

            // Execute the ETL query
            ResultSet etlResultSet = etlStatement.executeQuery(etlQuery);

            while (etlResultSet.next()) {
                String etlSegment = etlResultSet.getString("segment");
                LocalDateTime etlDateTime = LocalDateTime.parse(etlSegment, ETL_DATE_FORMATTER);
                int etlTotalTransitions = etlResultSet.getInt("total_transitions");
                etlSegmentsPlusTotalTransitionsMap.put(etlDateTime, etlTotalTransitions);
            }

            // Check for missing segments in Billing
            List<LocalDateTime> missedSegmentsInEtl = new ArrayList<>(billingSegmentsPlusCountMap.keySet());
            missedSegmentsInEtl.removeAll(etlSegmentsPlusTotalTransitionsMap.keySet());

            missedSegmentsInEtl.forEach(segment -> missingSegmentsInETL.add(segment));
            System.out.println();
            System.out.println("Total Billing Count: " + billingSegmentsPlusCountMap.values().stream().mapToInt(Integer::intValue).sum());
            System.out.println("Total ETL Total Transitions: " + etlSegmentsPlusTotalTransitionsMap.values().stream().mapToInt(Integer::intValue).sum());
            System.out.println();

            System.out.println();
            System.out.println("Missing segments in ETL:");
            missingSegmentsInETL.stream()
                    .map(segment -> segment.format(BILLING_DATE_FORMATTER))
                    .sorted()
                    .forEach(System.out::println);
            System.out.println();

            // Check for missing segments in ETL
            List<LocalDateTime> missedSegmentsInBilling = new ArrayList<>(etlSegmentsPlusTotalTransitionsMap.keySet());
            missedSegmentsInBilling.removeAll(billingSegmentsPlusCountMap.keySet());

            missedSegmentsInBilling.forEach(segment -> missingSegmentsInBilling.add(segment));

            System.out.println("Missing segments in Billing:");
            missingSegmentsInBilling.stream()
                    .map(segment -> segment.format(BILLING_DATE_FORMATTER))
                    .sorted()
                    .forEach(System.out::println);
            System.out.println();

            // Check for count and total_transitions mismatches
            etlSegmentsPlusTotalTransitionsMap.keySet().removeAll(missedSegmentsInBilling);

            List<LocalDateTime> sortedSegments = new ArrayList<>(etlSegmentsPlusTotalTransitionsMap.keySet());
            Collections.sort(sortedSegments);

            for (LocalDateTime segment : sortedSegments) {
                int billingCount = billingSegmentsPlusCountMap.getOrDefault(segment, 0);
                int etlTotalTransitions = etlSegmentsPlusTotalTransitionsMap.getOrDefault(segment, 0);
                if (Math.abs(billingCount - etlTotalTransitions) >= threshold) {
                    mismatchedSegments.add(segment.format(BILLING_DATE_FORMATTER));
                    mismatchedCounts.add(billingCount);
                    mismatchedTotalTransitions.add(etlTotalTransitions);
                    System.out.println("\n");
                    System.out.println("Mismatch found at segment: " + segment.format(BILLING_DATE_FORMATTER));
                    System.out.println("Billing Count: " + billingCount);
                    System.out.println("ETL Total Transitions: " + etlTotalTransitions);
                    System.out.println();

                    // Compare keys between Billing and ETL
                    Map<String, Integer> billingKeys = new HashMap<>();
                    Map<String, Integer> etlKeys = new HashMap<>();

                    // Retrieve Billing keys for this segment
                    String billingKeysQuery = "SELECT UPPER(CONCAT_WS('-', SUBSTRING(MD5(CONCAT(bp.id, '|vod-store|" + etlType + "|', bp.status)), 1, 8), SUBSTRING(MD5(CONCAT(bp.id, '|vod-store|" + etlType + "|', bp.status)), 9, 4), SUBSTRING(MD5(CONCAT(bp.id, '|vod-store|" + etlType + "|', bp.status)), 13, 4)," +
                            " SUBSTRING(MD5(CONCAT(bp.id, '|vod-store|" + etlType + "|', bp.status)), 17, 4), SUBSTRING(MD5(CONCAT(bp.id, '|vod-store|" + etlType + "|', bp.status)), 21))) AS id, bp.* " +
                            "FROM b_processes bp " +
                            "WHERE bp.carrier = " + billingCarrierId + " " +
                            "AND moddate >= '" + segment.format(BILLING_DATE_FORMATTER) + "' " +
                            "AND moddate < '" + segment.plusMinutes(1).format(BILLING_DATE_FORMATTER) + "' " +
                            "AND type = '" + billingType + "' " +
                            "AND status IN ('successful', 'failed');";

                    // Retrieve ETL keys for this segment
                    String etlKeysQuery = "SELECT * " +
                            "FROM user_transitions ut " +
                            "LEFT JOIN user_stories us ON us.uuid = ut.user_story_id " +
                            "WHERE us.carrier_offer_id = " + etlCarrierOfferId + " " +
                            "AND ut.[type] = '" + etlType + "' " +
                            "AND ut.[transitioned_at] >= '" + segment.format(ETL_DATE_FORMATTER) + "' " +
                            "AND ut.[transitioned_at] < '" + segment.plusMinutes(1).format(ETL_DATE_FORMATTER) + "' " +
                            "AND ut.[status] IN('successful','failed') " ;

                    ResultSet etlKeysResultSet = etlStatement.executeQuery(etlKeysQuery);
                    ResultSet billingKeysResultSet = billingStatement.executeQuery(billingKeysQuery);
                    while (billingKeysResultSet.next()) {
                        String key = billingKeysResultSet.getString("id");
                        billingKeys.put(key,0);
                    }
                    while (etlKeysResultSet.next()) {
                        String key = etlKeysResultSet.getString("uuid");
                        etlKeys.put(key,0);
                    }
                    for (String key : billingKeys.keySet()) {
                        if (!etlKeys.containsKey(key)) {
                            System.out.println("Key without a match in ETL: " + key);
                        }
                    }
                    // Find keys missing in Billing
                    for (String key : etlKeys.keySet()) {
                        if (!billingKeys.containsKey(key)) {
                            System.out.println("Key without a match in Billing: " + key);
                        }
                    }
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

