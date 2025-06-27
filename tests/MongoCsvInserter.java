import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.bson.Document;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MongoCsvInserter {

    // IMPORTANT: This URI must point to your mongos router.
    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String DATABASE_NAME = "user_db";
    private static final String COLLECTION_NAME = "users";
    private static final String SHARD_KEY_FIELD = "country";

    public static void main(String[] args) {
        String csvFilePath = "data/users.csv";

        try (MongoClient mongoClient = MongoClients.create(MONGO_URI)) {
            setupSharding(mongoClient);

            MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);
            
            collection.deleteMany(new Document());
            System.out.println("Cleared all documents from sharded collection: " + COLLECTION_NAME);

            insertCsvData(collection, csvFilePath);
            
            System.out.println("-------------------------------------------");
            System.out.println("Verification:");
            System.out.println("Total documents in sharded collection '" + COLLECTION_NAME + "': " + collection.countDocuments());
            System.out.println("-------------------------------------------");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void setupSharding(MongoClient mongoClient) {
        MongoDatabase adminDb = mongoClient.getDatabase("admin");
        
        try {
            adminDb.runCommand(new Document("enableSharding", DATABASE_NAME));
            System.out.println("Sharding enabled for database: " + DATABASE_NAME);
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == 23) { // AlreadyInitialized
                System.out.println("Sharding was already enabled for database: " + DATABASE_NAME);
            } else {
                throw e;
            }
        }
        
        try {
            Document shardKey = new Document(SHARD_KEY_FIELD, 1);
            Document shardCollectionCmd = new Document("shardCollection", DATABASE_NAME + "." + COLLECTION_NAME)
                                            .append("key", shardKey);
            adminDb.runCommand(shardCollectionCmd);
            System.out.println("Collection sharded: " + COLLECTION_NAME + " on key: " + SHARD_KEY_FIELD);
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == 20) { // NamespaceExists
                 System.out.println("Collection was already sharded: " + COLLECTION_NAME);
            } else {
                throw e;
            }
        }
    }

    private static void insertCsvData(MongoCollection<Document> collection, String filePath) {
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            String[] headers = reader.readNext();
            if (headers == null) {
                System.out.println("CSV file is empty or header is missing.");
                return;
            }

            List<Document> documents = new ArrayList<>();
            List<String[]> allRows = reader.readAll();

            for (String[] row : allRows) {
                Document doc = new Document();
                for (int i = 0; i < headers.length; i++) {
                    if (headers[i].equalsIgnoreCase("id")) {
                        try {
                            doc.append(headers[i], Integer.parseInt(row[i]));
                        } catch (NumberFormatException e) {
                             doc.append(headers[i], row[i]);
                        }
                    } else {
                        doc.append(headers[i], row[i]);
                    }
                }
                documents.add(doc);
            }
            
            if (!documents.isEmpty()) {
                collection.insertMany(documents);
                System.out.println("Successfully inserted " + documents.size() + " documents into MongoDB.");
            }

        } catch (IOException | CsvException e) {
            System.err.println("Error processing CSV file: " + e.getMessage());
        }
    }
}
