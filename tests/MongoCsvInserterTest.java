import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;

/**
 * Integration test for the MongoCsvInserter class.
 *
 * NOTE: This test requires a running MongoDB sharded cluster router (mongos)
 * on mongodb://localhost:27017.
 */
class MongoCsvInserterTest {

    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String DATABASE_NAME = "user_db";
    private static final String COLLECTION_NAME = "users";
    private final File tempCsvFile = new File("users.csv");

    /**
     * Creates a temporary CSV file with test data before each test.
     */
    @BeforeEach
    void setUp() throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(tempCsvFile))) {
            writer.println("id,name,email,country");
            writer.println("101,Alice,alice@test.com,USA");
            writer.println("102,Bob,bob@test.com,Canada");
            writer.println("103,Charlie,charlie@test.com,UK");
        }
    }

    /**
     * Deletes the temporary CSV file after each test.
     */
    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(tempCsvFile.toPath());
    }

    @Test
    void testMainInsertsCsvDataCorrectly() {
        // Execute the main method from the class we are testing.
        MongoCsvInserter.main(new String[0]);

        // Connect to MongoDB to verify the results.
        try (MongoClient mongoClient = MongoClients.create(MONGO_URI)) {
            MongoCollection<Document> collection = mongoClient
                    .getDatabase(DATABASE_NAME)
                    .getCollection(COLLECTION_NAME);

            // 1. Verify that the correct number of documents were inserted.
            assertEquals(3, collection.countDocuments(), "Should be 3 documents in the collection.");

            // 2. Verify the content of a specific document.
            Document aliceDoc = collection.find(new Document("name", "Alice")).first();
            assertNotNull(aliceDoc, "Alice's document should exist.");
            assertEquals(101, aliceDoc.getInteger("id"));
            assertEquals("alice@test.com", aliceDoc.getString("email"));
            assertEquals("USA", aliceDoc.getString("country"));
            
            Document charlieDoc = collection.find(new Document("name", "Charlie")).first();
            assertNotNull(charlieDoc, "Charlie's document should exist.");
            assertEquals(103, charlieDoc.getInteger("id"));
            assertEquals("UK", charlieDoc.getString("country"));
        }
    }
}
