import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ShardedCsvInserterTest {

    private static final String TEST_CSV_PATH = "test_users.csv";
    private static final int NUM_SHARDS = 3; // Must match the value in ShardedCsvInserter

    @BeforeEach
    void setUp() throws IOException {
        try (FileWriter writer = new FileWriter(TEST_CSV_PATH)) {
            writer.write("id,name,email,country\n");
            writer.write("1,Test User A,a@test.com,USA\n");
            writer.write("2,Test User B,b@test.com,Canada\n");
            writer.write("3,Test User C,c@test.com,Germany\n");
            writer.write("4,Test User D,d@test.com,USA\n");
            writer.write("5,Test User E,e@test.com,Canada\n");
            writer.write("6,Test User F,f@test.com,USA\n");
        }
    }

    @AfterEach
    void tearDown() {
        new File(TEST_CSV_PATH).delete();
    }

    @Test
    @DisplayName("Should process CSV and shard data correctly across databases")
    @SuppressWarnings("unchecked")
    void testMainShardingProcess() {
        Map<Integer, Connection> connections = null;
        try {
            ShardedCsvInserter.initializeDatabaseShards();
            ShardedCsvInserter.processCsv(TEST_CSV_PATH);

            // Use Reflection to access the private static map of connections for verification
            Field connectionsField = ShardedCsvInserter.class.getDeclaredField("shardConnections");
            connectionsField.setAccessible(true);
            connections = (Map<Integer, Connection>) connectionsField.get(null);

            assertNotNull(connections);
            assertEquals(NUM_SHARDS, connections.size());

            int usaShard = Math.abs("USA".hashCode() % NUM_SHARDS);
            int canadaShard = Math.abs("Canada".hashCode() % NUM_SHARDS);
            int germanyShard = Math.abs("Germany".hashCode() % NUM_SHARDS);

            long totalRows = 0;
            for (int i = 0; i < NUM_SHARDS; i++) {
                List<String> countriesInShard = getCountriesFromShard(connections.get(i));
                totalRows += countriesInShard.size();
                
                if (i == usaShard) {
                    assertTrue(countriesInShard.stream().allMatch(c -> c.equals("USA")));
                    assertEquals(3, countriesInShard.size());
                } else if (i == canadaShard) {
                    assertTrue(countriesInShard.stream().allMatch(c -> c.equals("Canada")));
                    assertEquals(2, countriesInShard.size());
                } else if (i == germanyShard) {
                     assertTrue(countriesInShard.stream().allMatch(c -> c.equals("Germany")));
                    assertEquals(1, countriesInShard.size());
                } else {
                    assertEquals(0, countriesInShard.size());
                }
            }

            assertEquals(6, totalRows, "Total rows across shards should match the CSV file");

        } catch (Exception e) {
            fail("Test failed due to an exception: " + e.getMessage());
        } finally {
             if (connections != null) {
                connections.values().forEach(conn -> {
                    try {
                        if (conn != null && !conn.isClosed()) conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
    }

    private List<String> getCountriesFromShard(Connection conn) throws SQLException {
        List<String> countries = new ArrayList<>();
        assertNotNull(conn);
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT country FROM users")) {
            while (rs.next()) {
                countries.add(rs.getString("country"));
            }
        }
        return countries;
    }
}
