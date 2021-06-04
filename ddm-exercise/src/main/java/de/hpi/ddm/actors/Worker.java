package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.stream.Collectors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import de.hpi.ddm.structures.BloomFilter;
import de.hpi.ddm.systems.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import akka.cluster.Member;
import akka.cluster.MemberStatus;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
		this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class WelcomeMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private BloomFilter welcomeData;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CrackPasswordMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609599L;
		private Master.Password password;
		private int passwordLength;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CrackHintMessage implements Serializable {
		private static final long serialVersionUID = 8343942740408609599L;
		private Master.Hint hint;
		private List<Character> usedChars;
	}

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	private final ActorRef largeMessageProxy;
	private long registrationTime;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(WelcomeMessage.class, this::handle)
				.match(CrackPasswordMessage.class, this::handle)
				.match(CrackHintMessage.class, this::handle)
				// TODO: Add further messages here to share work between Master and Worker actors
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());
			
			this.registrationTime = System.currentTimeMillis();
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}
	
	private void handle(WelcomeMessage message) {
		final long transmissionTime = System.currentTimeMillis() - this.registrationTime;
		this.log().info("WelcomeMessage with " + message.getWelcomeData().getSizeInMB() + " MB data received in " + transmissionTime + " ms.");
	}

	private void handle(CrackHintMessage message) {
		for (Character missingCharacter : message.usedChars) {
			char[] characterSubset = message.usedChars.stream().map(Object::toString).filter(c -> !c.equals(missingCharacter.toString())).collect(Collectors.joining()).toCharArray();
			String crackedHint = heapPermutation(characterSubset, characterSubset.length, message.hint.hashedHint);
			if (crackedHint != null) {
				this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(new Master.CrackedHintMessage(message.hint.passwordId, missingCharacter), this.sender()), this.self());
				break;
			}
		}
	}
	
	private void handle(CrackPasswordMessage message) {
		char[] characterSubset = message.password.includedChars.stream().map(Object::toString).collect(Collectors.joining()).toCharArray();
		String crackedPassword = printAllKLengthRec(characterSubset, "", characterSubset.length, message.passwordLength, message.password.hashedPassword);
		if (crackedPassword != null) {
			this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(new Master.CrackedPasswordMessage(message.password, crackedPassword), this.sender()), this.self());
		}
	}
	
	private String hash(String characters) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(characters).getBytes("UTF-8"));
			
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private String heapPermutation(char[] a, int size, String hashedHint) {
		// If size is 1, store the obtained permutation
		if (size == 1) {
			String permutation = new String(a);
			if (hashedHint.equals(hash(permutation))) {
				return permutation;
			}
		}

		for (int i = 0; i < size; i++) {
			String result = heapPermutation(a, size - 1, hashedHint);
			if (result != null) {
				return result;
			}
			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
		return null;
	}

	// Print all possible strings of length k that can be formed from a set of n characters
	// https://www.geeksforgeeks.org/print-all-combinations-of-given-length/
	private String printAllKLengthRec(char[] set, String prefix, int n, int k, String hashedPassword) {
    // Base case: k is 0,
    // print prefix
    if (k == 0) {
			if (hashedPassword.equals(hash(prefix))) {
				return prefix;
			}
			return null;
    }
 
    // One by one add all characters
    // from set and recursively
    // call for k equals to k-1
    for (int i = 0; i < n; ++i) {

			// Next character of input added
			String newPrefix = prefix + set[i];
				
			// k is decreased, because
			// we have added a new character
			String result = printAllKLengthRec(set, newPrefix, n, k - 1, hashedPassword);
			if (result != null){
				return result;
			}
    }
		return null;
	}
}