import com.opencsv.CSVReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * This application demonstrates sharded insertion of data from a CSV file into multiple SQL database shards
 * using an in-memory H2 database. Sharding is based on the 'country' column from the CSV.
 */
public class ShardedCsvInserter {

  // Modify the static value according to your local environment setup for this program to run.
  // Number of database shards.
    private static final int NUM_SHARDS = 3;
    // Map to hold a connection for each shard.
    private static final Map<Integer, Connection> shardConnections = new HashMap<>();

    private static final String DB_URL_BASE = "db_url";
    private static final String DB_USER = "db_username";
    private static final String DB_PASSWORD = "db_pwd";

    public static void main(String[] args) {
        String csvFilePath = "input.csv";
        
        try {
            initializeDatabaseShards();
            processCsv(csvFilePath);
            verifyInsertion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConnections();
        }
    }

    private static void initializeDatabaseShards() throws SQLException {
        System.out.println("Initializing " + NUM_SHARDS + " database shards...");
        for (int i = 0; i < NUM_SHARDS; i++) {
            String dbUrl = DB_URL_BASE + i;
            Connection conn = DriverManager.getConnection(dbUrl, DB_USER, DB_PASSWORD);
            shardConnections.put(i, conn);

            String createTableSQL = "CREATE TABLE users (" +
                                    "id INT PRIMARY KEY, " +
                                    "name VARCHAR(255), " +
                                    "email VARCHAR(255), " +
                                    "country VARCHAR(255))";

            try (Statement stmt = conn.createStatement()) {
                stmt.execute(createTableSQL);
            }
        }
        System.out.println("All shards initialized successfully.\n");
    }

    private static void processCsv(String filePath) {
        System.out.println("Processing CSV file: " + filePath + "\n");
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            reader.readNext(); // Skip header row

            Map<Integer, PreparedStatement> preparedStatements = new HashMap<>();
            Map<Integer, Integer> batchCounts = new HashMap<>();
            String insertSQL = "INSERT INTO users (id, name, email, country) VALUES (?, ?, ?, ?)";

            for (int i = 0; i < NUM_SHARDS; i++) {
                preparedStatements.put(i, shardConnections.get(i).prepareStatement(insertSQL));
                batchCounts.put(i, 0);
            }

            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                int id = Integer.parseInt(nextLine[0]);
                String name = nextLine[1];
                String email = nextLine[2];
                String country = nextLine[3];

                // --- SHARDING LOGIC ---
                // Determine the shard ID based on the country's hash code.
                int shardId = Math.abs(country.hashCode() % NUM_SHARDS);
                System.out.println("Record ID " + id + " ('" + country + "') -> Shard " + shardId);
                
                PreparedStatement pstmt = preparedStatements.get(shardId);
                pstmt.setInt(1, id);
                pstmt.setString(2, name);
                pstmt.setString(3, email);
                pstmt.setString(4, country);
                pstmt.addBatch();

                batchCounts.put(shardId, batchCounts.get(shardId) + 1);
                if (batchCounts.get(shardId) >= 100) {
                    pstmt.executeBatch();
                    batchCounts.put(shardId, 0);
                }
            }
            
            // Execute remaining batches.
            System.out.println("\nExecuting final batches...");
            for (int i = 0; i < NUM_SHARDS; i++) {
                if (batchCounts.get(i) > 0) {
                    preparedStatements.get(i).executeBatch();
                }
                preparedStatements.get(i).close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void verifyInsertion() {
        System.out.println("\n--- Verifying Data in Shards ---");
        for (int i = 0; i < NUM_SHARDS; i++) {
            System.out.println("--- Shard " + i + " Data ---");
            String query = "SELECT * FROM users";
            try (Statement stmt = shardConnections.get(i).createStatement();
                 java.sql.ResultSet rs = stmt.executeQuery(query)) {
                
                boolean found = false;
                while (rs.next()) {
                    found = true;
                    System.out.printf("ID: %d, Name: %s, Country: %s\n",
                        rs.getInt("id"), rs.getString("name"), rs.getString("country"));
                }
                if (!found) {
                    System.out.println("No data found in this shard.");
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void closeConnections() {
        System.out.println("\nClosing all shard connections...");
        shardConnections.values().forEach(conn -> {
            try {
                if (conn != null && !conn.isClosed()) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        System.out.println("Connections closed.");
    }
}
