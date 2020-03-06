package lab.experimental.publisher;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;
import lombok.extern.slf4j.Slf4j;
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

	private static Queue<String> broker = new LinkedList<>();
	private static StringBuilder event = new StringBuilder();
	public static final String NEWLINE = System.lineSeparator();

	@Override
	public void run(String... args) throws InterruptedException {
		log.info("EXECUTING : command line runner");

		Thread pub = new Thread(this::publish);
		pub.setName("PUBLISHER");

		pub.start();
	}

	private void publish() {
		Scanner session = new Scanner(System.in);
		while (true) {
			String message = session.nextLine();

			if (message.equals("stop")) {
				broker.add(message);
				log.info(event.toString());
				break;
			}

			event.append(Thread.currentThread().getName())
					.append(" | Published -> ")
					.append(message)
					.append(NEWLINE);

			broker.add(message);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				log.error(e.getMessage());
				Thread.currentThread().interrupt();
			}
		}
	}
}
