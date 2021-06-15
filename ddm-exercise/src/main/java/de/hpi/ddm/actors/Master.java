package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.ddm.structures.BloomFilter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
		return Props.create(Master.class, () -> new Master(reader, collector, welcomeData));
	}

	public Master(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
		this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
		this.welcomeData = welcomeData;
		this.readingComplete = false;
		this.idleWorkers = new LinkedList<>();
		this.passwordTasks = new LinkedList<>();
		this.passwordTaskBuffer = new HashMap<>();
		this.hintTasks = new LinkedList<>();
		this.initialized = false;
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CrackedPasswordMessage implements Serializable {
		private static final long serialVersionUID = 1000000000000000000L;
		private Password password;
		private String crackedPassword;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CrackedHintMessage implements Serializable {
		private static final long serialVersionUID = 1000000000000000001L;
		private int passwordId;
		private Character missingCharacter;
	}
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private final ActorRef largeMessageProxy;
	private final BloomFilter welcomeData;

	private long startTime;
	private LinkedList<Password> passwordTasks;
	private HashMap<Integer, Password> passwordTaskBuffer;
	private LinkedList<Hint> hintTasks;
	private List<Character> usedChars;
	private int passwordLength;
	private LinkedList<ActorRef> idleWorkers;
	private Boolean readingComplete;
	private Boolean initialized;
	
	@Data
	public static class Password {
		int id;
		String name;
		String hashedPassword;
		int amountOfUncrackedHints;
		List<Character> includedChars;
	}

	@Data
	public static class Hint {
		int passwordId;
		String hashedHint;
	}

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.match(CrackedPasswordMessage.class, this::handle)
				.match(CrackedHintMessage.class, this::handle)
				// TODO: Add further messages here to share work between Master and Worker actors
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();

		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void handle(BatchMessage message) {

		// TODO: This is where the task begins:
		// - The Master received the first batch of input records.
		// - To receive the next batch, we need to send another ReadMessage to the reader.
		// - If the received BatchMessage is empty, we have seen all data for this task.
		// - We need a clever protocol that forms sub-tasks from the seen records, distributes the tasks to the known workers and manages the results.
		//   -> Additional messages, maybe additional actors, code that solves the subtasks, ...
		//   -> The code in this handle function needs to be re-written.
		// - Once the entire processing is done, this.terminate() needs to be called.
		
		// Info: Why is the input file read in batches?
		// a) Latency hiding: The Reader is implemented such that it reads the next batch of data from disk while at the same time the requester of the current batch processes this batch.
		// b) Memory reduction: If the batches are processed sequentially, the memory consumption can be kept constant; if the entire input is read into main memory, the memory consumption scales at least linearly with the input size.
		// - It is your choice, how and if you want to make use of the batched inputs. Simply aggregate all batches in the Master and start the processing afterwards, if you wish.

		// TODO: Stop fetching lines from the Reader once an empty BatchMessage was received; we have seen all data then
		List<String[]> currrentLines = message.getLines();
		if (currrentLines.isEmpty()) {
			this.readingComplete = true;
			return;
		}
		if (!this.initialized){
			this.usedChars = new String(currrentLines.get(0)[2]).chars().mapToObj(c -> (char) c).collect(Collectors.toList());;
			this.passwordLength = Integer.parseInt(currrentLines.get(0)[3]);
			this.initialized = true;
		}

		// TODO: Process the lines with the help of the worker actors
		for (String[] line : message.getLines()) {
			String[] hints = Arrays.copyOfRange(line, 5, line.length);
			Password currentPassword = new Password();
			currentPassword.id = Integer.parseInt(line[0]) - 1;
			currentPassword.name = line[1];
			currentPassword.hashedPassword = line[4];
			currentPassword.amountOfUncrackedHints = hints.length;
			currentPassword.includedChars = new LinkedList<Character>(this.usedChars);
			this.passwordTaskBuffer.put(currentPassword.id, currentPassword);

			for (String hint : hints){
				Hint currentHint = new Hint();
				currentHint.passwordId = currentPassword.id;
				currentHint.hashedHint = hint;
				this.hintTasks.add(currentHint);
			}
		}
		this.assignAvailableWorkers();
		
		// TODO: Fetch further lines from the Reader
		this.reader.tell(new Reader.ReadMessage(), this.self());
		
	}
	
	private void assignAvailableWorkers() {
		if(this.readingComplete && this.passwordTaskBuffer.size() == 0 && this.passwordTasks.size() == 0){
			this.terminate();
		}
		while (this.idleWorkers.size() > 0 && (this.passwordTasks.size() > 0 || this.hintTasks.size() > 0)){
			ActorRef worker = this.idleWorkers.removeFirst();
			if(this.passwordTasks.size() > 0){
				this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(new Worker.CrackPasswordMessage(passwordTasks.removeFirst(), this.passwordLength), worker), this.self());
			} else {
				this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(new Worker.CrackHintMessage(hintTasks.removeFirst(), this.usedChars), worker), this.self());
			}
		}

	}

	protected void handle(CrackedHintMessage message) {
		Password currentPassword = passwordTaskBuffer.get(message.getPasswordId());
		currentPassword.includedChars.remove(message.missingCharacter);
		currentPassword.amountOfUncrackedHints--;
		if (currentPassword.amountOfUncrackedHints > 0) {
			passwordTaskBuffer.put(currentPassword.id, currentPassword);
		} else {
			passwordTaskBuffer.remove(currentPassword.id);
			passwordTasks.add(currentPassword);
		}
		this.idleWorkers.add(this.sender());
		this.assignAvailableWorkers();
	}

	protected void handle(CrackedPasswordMessage message) {
		this.collector.tell(new Collector.CollectMessage(message.getPassword().getName() + "'s password is " + message.getCrackedPassword()), this.self());
		this.passwordTasks.remove(message.getPassword());
		this.idleWorkers.add(this.sender());
		this.assignAvailableWorkers();
	}

	protected void terminate() {
		this.collector.tell(new Collector.PrintMessage(), this.self());
		
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());
		this.idleWorkers.add(this.sender());
		this.log().info("Registered {}", this.sender());
		
		this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(new Worker.WelcomeMessage(this.welcomeData), this.sender()), this.self());
		
		// TODO: Assign some work to registering workers. Note that the processing of the global task might have already started.
		this.assignAvailableWorkers();
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		this.log().info("Unregistered {}", message.getActor());
	}
}
