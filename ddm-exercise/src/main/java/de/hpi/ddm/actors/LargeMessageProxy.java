package de.hpi.ddm.actors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	static final int CHUNK_SIZE = 1024*1024;
	private byte[][] globalMessageStore = new byte[0][0];
	private int messagesReceived = 0;
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
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class PartialBytesMessage implements Serializable {
		private static final long serialVersionUID = 4057807739472319842L;
		private byte[] bytes;
		private ActorRef sender;
		private ActorRef receiver;
		private int id;
		private int amount;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
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
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream out = null;
		byte[] byteMessage = new byte[0];
		try {
			out = new ObjectOutputStream(bos);   
			out.writeObject(message);
			out.flush();
			byteMessage = bos.toByteArray();
		} catch (IOException ex) {
			System.out.println(ex);
		} finally {
			if (bos != null) {
				try {
					bos.close();
				} catch (IOException ex) {
					// ignore close exception
					System.out.println(ex);
				}
			}
		}
		int amountOfChunks = (int)Math.ceil(byteMessage.length / (double)CHUNK_SIZE);
		int idx = 0;

		for(int i = 0; i < amountOfChunks; i++) {
				byte[] msg = Arrays.copyOfRange(byteMessage,idx, idx + CHUNK_SIZE);
				idx += CHUNK_SIZE;
				receiverProxy.tell(new PartialBytesMessage(msg, sender, receiver, i, amountOfChunks), this.self());
        }
	}

	private void handle(PartialBytesMessage message) throws IOException, ClassNotFoundException {
		// TODO: With option a): Store the message, ask for the next chunk and, if all chunks are present, reassemble the message's content, deserialize it and pass it to the receiver.
		// The following code assumes that the transmitted bytes are the original message, which they shouldn't be in your proper implementation ;-)
		if (globalMessageStore.length == 0) {
			globalMessageStore = new byte[message.amount][CHUNK_SIZE];
		}
		messagesReceived += 1;
		globalMessageStore[message.id] = message.bytes;

		if (messagesReceived == message.amount) {
			byte[] newMessage = new byte[0];
			for (byte[] msg : globalMessageStore) {
				System.arraycopy(msg, 0, newMessage, newMessage.length, msg.length);
			}
			LargeMessage<?> deserializedMessage;
			ByteArrayInputStream bis = new ByteArrayInputStream(newMessage);
			ObjectInput in = new ObjectInputStream(bis);
			deserializedMessage = (LargeMessage<?>) in.readObject();
			globalMessageStore = new byte[0][0];
			messagesReceived = 0;
			message.getReceiver().tell(deserializedMessage, message.getSender());
		}
	}
}
