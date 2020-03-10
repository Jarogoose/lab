package lab.experimental.subscriber;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import java.util.Timer;
import java.util.TimerTask;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class SubscriberApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(SubscriberApplication.class, args);
		log.info("Subscriber is up and running ...");
	}

	@Override
	public void run(String... args) throws InterruptedException {
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				log.info("SUBSCRIBER SCHEDULER STARTED");
				subscribe();
				log.info("SUBSCRIBER SCHEDULER ENDED");
			}
		};

		Timer timer = new Timer();
		final long startAt = 0;
		final long delay = 10000; // milliseconds
		timer.scheduleAtFixedRate(task, startAt, delay);
	}

	private void subscribe() {
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("broker");

		MongoCollection<Document> collection = db.getCollection("queue");

		long thirtySecOld = System.currentTimeMillis() - 60 * 1000 / 2;
		FindIterable<Document> oldData = collection.find(Filters.lt("time", thirtySecOld));

		for (Document doc : oldData) {
			collection.deleteOne(doc);
			log.info(String.format("Message %s was deleted", doc.toString()));
		}

		client.close();
	}
}
