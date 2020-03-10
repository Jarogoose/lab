package lab.experimental.publisher;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.Scanner;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class PublisherApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(PublisherApplication.class, args);
		log.info("Publisher is up and running ...");
	}

	@Override
	public void run(String... args) throws InterruptedException {
		Scanner session = new Scanner(System.in);
		boolean exit = false;

		while (!exit) {
			final String message = session.nextLine();
			if (message.equals("exit")) {
				exit = true;
			}
			publish(message);
			log.info(String.format("Message %s was published", message));
		}
	}

	private void publish(final String message) {
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("broker");

		MongoCollection<Document> collection = db.getCollection("queue");
		Document doc = new Document("time", System.currentTimeMillis()).append("message", message);
		collection.insertOne(doc);

		client.close();
	}
}
