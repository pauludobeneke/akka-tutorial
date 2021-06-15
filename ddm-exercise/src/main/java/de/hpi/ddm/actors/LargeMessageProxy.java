package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;

import com.twitter.chill.KryoPool;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.ddm.singletons.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	static final int CHUNK_SIZE = 1024*128;
	private KryoPool kryo = KryoPoolSingleton.get();

	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class PartialBytesMessage implements Serializable {
		private static final long serialVersionUID = 4057807739472319842L;
		private byte[] bytes;
		private int messageIdentifier;
		private int chunkIdentifier;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class InitializeMessage implements Serializable {
		private static final long serialVersionUID = 4031957807739472842L;
		private ActorRef sender;
		private ActorRef receiver;
		private int messageIdentifier;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class RequestNewChunkMessage implements Serializable {
		private static final long serialVersionUID = 4057831907739472842L;
		private int messageIdentifier;
		private int chunkIdentifier;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class FinishedMessage implements Serializable {
		private static final long serialVersionUID = 4007735783199472842L;
		private int messageIdentifier;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class MessageBuffer {
		private ActorRef sender;
		private ActorRef receiver;
		private byte[] messageBuffer;
	}

	/////////////////
	// Actor State //
	/////////////////
	private HashMap<Integer, byte[][]> sendMessagesState = new HashMap<Integer, byte[][]>();
	private HashMap<Integer, MessageBuffer> receivedMessagesState = new HashMap<Integer, MessageBuffer>();
	private int curMessageId = 0; 
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(PartialBytesMessage.class, this::handle)
				.match(InitializeMessage.class, this::handle)
				.match(RequestNewChunkMessage.class, this::handle)
				.match(FinishedMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> largeMessage) {
		Object message = largeMessage.getMessage();
		ActorRef sender = this.sender();
		ActorRef receiver = largeMessage.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
		
		// TODO: Implement a protocol that transmits the potentially very large message object.
		// The following code sends the entire message wrapped in a BytesMessage, which will definitely fail in a distributed setting if the message is large!
		// Solution options:
		// a) Split the message into smaller batches of fixed size and send the batches via ...
		//    a.a) self-build send-and-ack protocol (see Master/Worker pull propagation), or
		//    a.b) Akka streaming using the streams build-in backpressure mechanisms.
		// b) Send the entire message via Akka's http client-server component.
		// c) Other ideas ...
		// Hints for splitting:
		// - To split an object, serialize it into a byte array and then send the byte array range-by-range (tip: try "KryoPoolSingleton.get()").
		// - If you serialize a message manually and send it, it will, of course, be serialized again by Akka's message passing subsystem.
		// - But: Good, language-dependent serializers (such as kryo) are aware of byte arrays so that their serialization is very effective w.r.t. serialization time and size of serialized data.
		
		byte[] serializedMessage = kryo.toBytesWithClass(message);
		byte[][] serializedMessageChunks = splitArray(serializedMessage);
		sendMessagesState.put(curMessageId, serializedMessageChunks);
		receiverProxy.tell(new InitializeMessage(sender, receiver, curMessageId), this.getSelf());
		curMessageId++;
	}

	private void handle(InitializeMessage message) {
		this.receivedMessagesState.put(message.messageIdentifier, new MessageBuffer(message.sender, message.receiver, new byte[0]));
		this.getSender().tell(new RequestNewChunkMessage(message.messageIdentifier, 0), this.getSelf());
	}

	private void handle(RequestNewChunkMessage message) {
		byte[][] chunkedMessage = this.sendMessagesState.get(message.messageIdentifier);

		if (message.chunkIdentifier < chunkedMessage.length) {
			this.getSender().tell(new PartialBytesMessage(chunkedMessage[message.chunkIdentifier], message.messageIdentifier, message.chunkIdentifier), this.getSelf());
		}
		else {
			this.getSender().tell(new FinishedMessage(message.messageIdentifier), this.getSelf());
			this.sendMessagesState.remove(message.messageIdentifier);
		}
	}

	private void handle(PartialBytesMessage message) {
		// TODO: With option a): Store the message, ask for the next chunk and, if all chunks are present, reassemble the message's content, deserialize it and pass it to the receiver.
		// The following code assumes that the transmitted bytes are the original message, which they shouldn't be in your proper implementation ;-)
		MessageBuffer currentBuffer = receivedMessagesState.get(message.messageIdentifier);
		byte[] currentMessage = currentBuffer.messageBuffer;
		currentBuffer.messageBuffer = new byte[currentMessage.length + message.bytes.length];
		System.arraycopy(currentMessage, 0, currentBuffer.messageBuffer, 0, currentMessage.length);
    System.arraycopy(message.bytes, 0, currentBuffer.messageBuffer, currentMessage.length, message.bytes.length);
		this.getSender().tell(new RequestNewChunkMessage(message.messageIdentifier, message.chunkIdentifier + 1), this.getSelf());
	}

	private void handle(FinishedMessage message) {
		MessageBuffer buffer = receivedMessagesState.remove(message.messageIdentifier);
		buffer.receiver.tell(kryo.fromBytes(buffer.messageBuffer), buffer.sender);
	}

	private byte[][] splitArray(byte[] msg) {
		int amountOfChunks = (int)Math.ceil(msg.length / (double)CHUNK_SIZE);
		int idx = 0;
		int endIdx = 0;
		byte[][] result = new byte[amountOfChunks][CHUNK_SIZE];
		for(int i = 0; i < amountOfChunks; i++) {
			if (i == amountOfChunks - 1) {
				endIdx = idx + msg.length % CHUNK_SIZE;
			}
			else {
				endIdx = idx + CHUNK_SIZE;
			}
			result[i] = Arrays.copyOfRange(msg, idx, endIdx);
			idx += CHUNK_SIZE;
		}
		return result;
	}
}
