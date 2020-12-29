// Mongodb Database

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter a database name: ");
        String dbName = scanner.next();
        if(dbName.equals(""))
            dbName = "data-store";

        MongoClient mongoClient = new MongoClient("localhost", 27017);
        MongoCredential mongoCredential = MongoCredential.createCredential("root", dbName, "".toCharArray());
        System.out.println("Connected to the Database Successfully");

        MongoDatabase mongoDatabase = mongoClient.getDatabase(dbName);

        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("data-store");

        System.out.println("Enter your choice: \n1. Insert\n2. Read\n 3.Delete");
        int c = scanner.nextInt();

        if(c == 1){
        // Insertion
        System.out.println("Enter the key: ");
        String key = scanner.next();

        if(key.length()>32){
            System.out.println("Memory limit exceeded.");
            System.exit(0);
        }

        FindIterable<Document> findIterable = mongoCollection.find();
        for (Document value : findIterable) {
            if(value.get(key) != null) {
                System.out.println("Key already exists.");
                System.exit(0);
            }
        }

        System.out.println("Enter the value in json format: ");
        String value = scanner.nextLine();

        if(value.length()>16000){
            System.out.println("Memory limit exceeded.");
            System.exit(0);
        }

        Document document = new Document(key, value);
        mongoCollection.insertOne(document);}
        
        if(c == 2){
            // Reading
            System.out.println("Enter the key: ");
            String key1 = scanner.next();

            FindIterable<Document> findIterable = mongoCollection.find();
            for (Document value1 : findIterable) {
                if(value1.get(key1) != null) {
                    System.out.println("{" + key1 + ": {" + value1 + "}}");
                    System.exit(0);
                }
            }
        }
        
        if(c == 3){
            // Deleting
            System.out.println("Enter the key to delete: ");
            String key2 = scanner.next();
            Document document2 = null;

            FindIterable<Document> findIterable = mongoCollection.find();
            for (Document value2 : findIterable) {
                if(value2.get(key2) != null) {
                    document2 = value2;
                }
            }

            assert document2 != null;
            mongoCollection.deleteMany(document2);
        }

    }
}
