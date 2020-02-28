package br.com.dotofcodex.mongodb_sample;

import java.util.Arrays;
import java.util.Optional;
import java.util.regex.Pattern;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

/**
 * More information in the following link:
 * https://www.kenwalger.com/blog/nosql/mongodb-and-java/
 * https://docs.mongodb.com/manual/tutorial/enable-authentication/
 */
public class Main {

	private static final Logger logger = LoggerFactory.getLogger(Main.class);

	private static final String URI = "mongodb://localhost:27017";
	private static final String DATABASE = "mongodb_sample";

	private static final int ASCENDING = 1;
	private static final int DESCENDING = -1;
	
	public static void main(String[] args) {
		launch();
	}

	private static void launch() {
		logger.info("executing main method...");
		MongoDatabase database = getMongoDatabase(getMongoConnection());
		// insertOne(database);
		// insertMany(database);
		// dropCollection(database, "users");
		// countCollection(database, "users");
		// countBySexF(database);
		// find(database);
		// findFilters(database);
		// findWithCursor(database);
		// findOrderByNameAscending(database);
		// findOrderByNameAscendingLimit(database);
		// findByNameLikeOrderByNameDescending(database);
		// findOrderByNameDescending(database);
		// updateOne(database);
		// updateMany(database);
		// deleteOne(database);
		// deleteMany(database);
		logger.info("done");
	}

	private static MongoClient getMongoConnection() {
		logger.info("creating mongodb connection...");
		MongoClient client = new MongoClient(new MongoClientURI(URI));
		logger.info("connection created");
		return client;
	}

	private static MongoDatabase getMongoDatabase(MongoClient client) {
		logger.info("getting database access...");
		MongoDatabase database = client.getDatabase(DATABASE);
		logger.info("access stablished");
		return database;
	}

	private static void insertOne(MongoDatabase database) {
		logger.info("inserting...");
		MongoCollection<Document> users = database.getCollection("users");

		Document user = new Document();
		user
			.append("name", "Francisca")
			.append("email", "francisca.alves.silva.55@gmail.com")
			.append("password", "551");

		users.insertOne(user);

		logger.info("insert done");
	}

	private static void insertMany(MongoDatabase database) {
		logger.info("inserting many...");
		MongoCollection<Document> users = database.getCollection("users");

		Document francisca = new Document();
		francisca
			.append("name", "Francisca Alves")
			.append("email", "francisca.alves.silva.55@gmail.com")
			.append("password", "551");

		Document maisa = new Document();
		maisa
			.append("name", "Maisa Oliveira")
			.append("email", "maisaoliveira161@gmail.com")
			.append("password", "161");
		
		Document pedro = new Document();
		pedro
			.append("name", "Pedro Ferreira")
			.append("email", "pedroferreiracjr@gmail.com")
			.append("password", "123");
		
		Document mirela = new Document();
		mirela
			.append("name", "Mirela Brito")
			.append("email", "mirela.brito@gmail.com")
			.append("password", "121300");
		
		users.insertMany(Arrays.asList(francisca, maisa, pedro, mirela));

		logger.info("insert many done");
	}
	
	private static void dropCollection(MongoDatabase database, String collectionName) {
		logger.info("dropping collection...");
		MongoCollection<Document> collection = database.getCollection(collectionName);
		collection.drop();
		logger.info("drop done");
	}
	
	private static void countCollection(MongoDatabase database, String collectionName) {
		logger.info("counting collection...");
		MongoCollection<Document> collection = database.getCollection(collectionName);
		logger.info("there are " + collection.count() + " documents");
		logger.info("count done");
	}

	private static void countBySexF(MongoDatabase database) {
		logger.info("counting by sex equal to f...");
		MongoCollection<Document> collection = database.getCollection("users");
		long result = collection.count(Filters.eq("sex", "f"));
		logger.info("there are " + result + " that has sex property equals to 'f'");
	}
	
	private static void findOrderByNameAscending(MongoDatabase database) {
		logger.info("executing find order by...");
		MongoCollection<Document> collection = database.getCollection("users");
		collection.find().sort(new Document("name", ASCENDING)).forEach((Document user) -> {
			logger.info(user.get("name").toString());
		});
		logger.info("find order by done");
	}
	
	private static void findOrderByNameAscendingLimit(MongoDatabase database) {
		logger.info("executing find order by...");
		MongoCollection<Document> collection = database.getCollection("users");
		collection.find().sort(new Document("name", ASCENDING)).limit(1).forEach((Document user) -> {
			logger.info(user.get("name").toString());
		});
		logger.info("find order by done");
	}
	
	private static void findByNameLikeOrderByNameDescending(MongoDatabase database) {
		logger.info("executing find order by name like...");
		MongoCollection<Document> collection = database.getCollection("users");
		collection.find(Filters.regex("name", Pattern.compile("Mai."))).sort(new Document("name", DESCENDING)).forEach((Document user) -> {
			logger.info(user.toJson());
		});
		logger.info("find order by done");
	}
	
	private static void findOrderByNameDescending(MongoDatabase database) {
		logger.info("executing find order by...");
		MongoCollection<Document> collection = database.getCollection("users");
		collection.find().sort(new Document("name", DESCENDING)).forEach((Document user) -> {
			logger.info(user.get("name").toString());
		});
		logger.info("find order by done");
	}
	
	private static void find(MongoDatabase database) {
		logger.info("execution find...");
		MongoCollection<Document> collection = database.getCollection("users");
		collection.find().forEach((Document user) -> {
			logger.info(user.get("name").toString() + ": " + user.get("_id").toString());
		});
		logger.info("find done");
	}
	
	private static void findFilters(MongoDatabase database) {
		logger.info("execution find filters...");
		MongoCollection<Document> collection = database.getCollection("users");
		/*
		collection.find().forEach((Document user) -> {
			String name = user.get("name").toString();
			logger.info(name);
			
			if ("Pedro Ferreira".equals(name)) {
				logger.info("name matches");
				Document pedro = collection.find(Filters.eq("_id", user.get("_id"))).first();
				logger.info(pedro.get("email").toString());
				logger.info(user.get("_id").getClass().toString());
			}
		});
		 */
		Optional<Document> user = Optional.ofNullable(collection.find(Filters.eq("_id", new ObjectId("5e58971db731d31dfabd9bb7"))).first());
		user.ifPresent((Document value) -> {
			String id = value.get("_id").toString();
			String name = value.get("name").toString();
			String email = value.get("email").toString();
			String password = value.get("password").toString();
			logger.info(String.format("%s:%s:%s:%s", id, name, email, password));
		});
		
		logger.info("find done");
	}
	
	private static void updateOne(MongoDatabase database) {
		logger.info("execution update...");
		MongoCollection<Document> collection = database.getCollection("users");
		collection.updateOne(new Document("_id", new ObjectId("5e58971db731d31dfabd9bb7")), new Document().append("$set", new Document("sex", "m")));
		logger.info("update done");
	}
	
	private static void updateMany(MongoDatabase database) {
		logger.info("execution update many...");
		MongoCollection<Document> collection = database.getCollection("users");
		collection.updateMany(Filters.not(Filters.exists("sex")), new Document().append("$set", new Document("sex", "f")));
		logger.info("update many done");
	}
	
	private static void findWithCursor(MongoDatabase database) {
		logger.info("execution find with cursor...");
		MongoCollection<Document> collection = database.getCollection("users");
		try(MongoCursor<Document> cursor = collection.find().iterator()) {
			while(cursor.hasNext()) {
				logger.info(cursor.next().toJson());
			}
		}
		
		logger.info("find with cursor done");
	}
	
	private static void deleteOne(MongoDatabase database) {
		logger.info("execution delete...");
		MongoCollection<Document> collection = database.getCollection("users");
		collection.deleteOne(Filters.eq("_id", new ObjectId("5e58971db731d31dfabd9bb8")));
		logger.info("delete done");
	}
	
	private static void deleteMany(MongoDatabase database) {
		logger.info("execution delete many...");
		MongoCollection<Document> collection = database.getCollection("users");
		collection.deleteMany(Filters.eq("sex", "m"));
		logger.info("delete many done");
	}
}
