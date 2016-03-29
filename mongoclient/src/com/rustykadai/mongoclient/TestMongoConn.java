package com.rustykadai.mongoclient;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.junit.Test;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static java.util.Arrays.asList; 
import com.mongodb.client.model.Filters;

public class TestMongoConn {

	@Test
	public void test() {

		MongoClient mongoClient = null;
		MongoDatabase database = null;
		MongoCollection<Document> collection = null;
		Document doc = null;

		try {

			mongoClient = new MongoClient("192.168.1.116", 27017);

		}

		catch (Exception e)

		{
			System.out.println("Problem creating mongo client.");
			e.printStackTrace();

		}

		assertTrue(mongoClient != null);

		try {

			for (String dbName : mongoClient.listDatabaseNames()) {

				System.out.println("Found database: " + dbName);
				
				if (dbName.equals("mongotestdb"))	{
					
					// Purge test db if exists
					System.out.println("Resetting test database...");
					mongoClient.dropDatabase(dbName);
									
				}

			}
			
		    database = mongoClient.getDatabase("mongotestdb");
		    System.out.println("Created mongo database: mongotestdb");
		    assertFalse(database.equals(null));
		    assertTrue(database.getName().equals("mongotestdb"));

		}

		catch (Exception e)

		{
			System.out.println("Problem creating database.");
			e.printStackTrace();

		}

		try {
			collection = database.getCollection("test-docs");
		} catch (Exception e)

		{
			System.out.println("Problem creating collection.");
			e.printStackTrace();

		}

		
		// Create the country-language docs similar to RDBMS country-language tables (many-to-many)
		
		try {
			
			
			List<Document> docs = new ArrayList<Document>(); 
			
			docs.add(new Document("country","United States")
					.append("population", 319)
					.append("capital", "Washington")
					.append("languages", asList(
							new Document()
							.append("language", "English"),
							new Document()
							.append("language", "Spanish"))));
			
			docs.add(new Document("country","United Kingdom")
					.append("population", 64)
					.append("capital", "London")
					.append("languages", asList(
							new Document()
							.append("language", "English"))));
			
			docs.add(new Document("country","Thailand")
					.append("population", 67)
					.append("capital", "Bangkok")
					.append("languages", asList(
							new Document()
							.append("language", "Thai"),
							new Document()
							.append("language", "Thai-Issan"))));
			
			docs.add(new Document("country","Laos")
					.append("population", 7)
					.append("capital", "Vientiane")
					.append("languages", asList(
							new Document()
							.append("language", "Lao"),
							new Document()
							.append("language", "Thai"),
							new Document()
							.append("language", "Hmong"))));
			docs.add(new Document("country","Cambodia")
					.append("population", 15)
					.append("capital", "Phnom Penh")
					.append("languages", asList(
							new Document()
							.append("language", "Khmer"))));
			docs.add(new Document("country","Vietnam")
					.append("population", 90)
					.append("capital", "Hanoi")
					.append("languages", asList(
							new Document()
							.append("language", "Vietnamese"),
							new Document()
							.append("language", "Hmong"))));
			
			database.getCollection("countries").insertMany(docs);
										 
			FindIterable<Document> select_countries = database.getCollection("countries").find();
			
			// Look at the json
			System.out.println("Documents in Country Collection:");
			select_countries.forEach(new Block<Document>() {
			    @Override
			    public void apply(final Document document) {
			        System.out.println(document.toJson());
			    }
			});
			
			// Get the Lao speaking countries
			FindIterable<Document> select_language = database.getCollection("countries").find(new Document("languages.language","Lao"));
			
			// Look at the json
			System.out.println("Lao-speaking countries:");
			select_language.forEach(new Block<Document>() {
			    @Override
			    public void apply(final Document document) {
			        System.out.println(document.toJson());
			    }
			});
			
			
			// Get the Lao or Thai speaking countries
			FindIterable<Document> select_multi_language = database.getCollection("countries").find(new Document("$or",
					asList(new Document("languages.language","Lao"),new Document("languages.language","Thai"))));
			
			// Look at the json
			System.out.println("Lao-speaking countries:");
			select_multi_language.forEach(new Block<Document>() {
			    @Override
			    public void apply(final Document document) {
			        System.out.println(document.toJson());
			    }
			});
		}
		

		catch (Exception e)

		{
			System.out.println("Problem creating document.");
			e.printStackTrace();

		}
				
		

//		try {
//			collection.insertOne(doc);
//			System.out.println(collection.count());
//		} catch (Exception e) {
//
//			System.out.println("Problem inserting document");
//			e.printStackTrace();
//
//		}

	}
}
