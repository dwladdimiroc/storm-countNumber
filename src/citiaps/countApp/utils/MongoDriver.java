package citiaps.countApp.utils;

import org.bson.Document;
import java.util.List;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDriver {
	private String host; // database host
	private int port; // database port
	private String dbName; // database name
	private String collectionName;
	private MongoClient mongo;
	private MongoDatabase db;
	private MongoCollection<Document> collection;

	private int status; // 0 for not initialized, 1 for initialized, -1 for
						// error

	public MongoDriver(String host, int port, String dbName) {
		this.host = host;
		this.port = port;
		this.dbName = dbName;
		this.collectionName = "default";
		this.status = 0;
	}

	public void insertOne(Document object) {
		this.collection.insertOne(object);
	}

	public void insertMany(List<? extends Document> objects) {
		this.collection.insertMany(objects);
	}
	
	public void find(){
		for(Document doc : this.collection.find()){
			// Código
		}
	}

	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}

	public void changeCollection(String collectionName) {
		this.collectionName = collectionName;
		this.collection = this.db.getCollection(this.collectionName);
	}

	// Setup the Connection with the Database
	public void setupMongo() {
		this.mongo = new MongoClient(this.host, this.port);
		this.db = this.mongo.getDatabase(this.dbName);
		this.collection = this.db.getCollection(this.collectionName);
		this.status = 1;
	}

	public void disconnect() {
		this.mongo.close();
	}

}