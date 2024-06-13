package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.lang.Thread;
import java.util.Collections;
import java.util.HashMap;
import java.io.IOException;

public class QuorumBasedTotalOrder {

	// General Configuration
	final static int N_PARTICIPANTS = 3;
	final static int QUORUM = (N_PARTICIPANTS / 2) + 1;

	public static class UpdateIdentifier {
		public int e;
		public int i;

		public UpdateIdentifier(int e, int i) {
			this.e = e;
			this.i = i;
		}

		@Override
		public int hashCode() {
			return Objects.hash(e, i);
		}

		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return "(" + this.e + ", " + this.i + ")";
		}
	}

	public static class Update {
		UpdateIdentifier identifier;
		public int v;

		public Update(UpdateIdentifier identifier, int v) {
			this.identifier = identifier;
			this.v = v;
		}
	}

	public static class PendingUpdateTuple {
		ArrayList<ActorRef> actors;
		int value;

		public PendingUpdateTuple(int value) {
			this.value = value;
			actors = new ArrayList<>();
		}
	}

	public enum Operation {
		READ, WRITE
	}

	// Timeouts Configuration
	final static int WRITEOK_TIMEOUT = 3000; // timeout started after receiving the update msg. Detects a coordinator crash
	final static int BROADCAST_INITIATE_TIMEOUT = 1000; // timeout started after sending a write request. Detects a coordinator crash
	final static int HEARTHBEAT_TIMEOUT = 10000; // timeout started after receiving a heartbeat msg. Detects a coordinator crash
	final static int REPLICA_TIMEOUT = 2000; // timeout started after sending a msg to a replica. Detects a replica crash

	// Messages Configuration
	public static class StartMessage implements Serializable { // Start message that sends the list of participants to everyone
		public final List<ActorRef> group;

		public StartMessage(List<ActorRef> group) {
			this.group = Collections.unmodifiableList(new ArrayList<>(group));
		}
	}

	public static class IssueWrite implements Serializable { // Sent from a client to a replica. Asks for a Write operation
		public int value;

		IssueWrite(int value) {
			this.value = value;
		};
	}

	public static class IssueRead implements Serializable {
	} // Sent from a client to a replica. Asks for a Read operation

	public static class ForwardUpdate implements Serializable { // Sent from a replica to the coordinator. Forward an update request
		public final Update update;

		ForwardUpdate(int e, int i, int value) {
			UpdateIdentifier identifier = new UpdateIdentifier(e, i);
			update = new Update(identifier, value);
		};
	}

	public static class UpdateRequest implements Serializable { // Sent from the coordinator to all replicas. Initiate the update protocol
		public final Update update;

		UpdateRequest(Update update) {
			this.update = update;
		};
	}

	public static class UpdateResponse implements Serializable {
		public final UpdateIdentifier updateId;

		UpdateResponse(UpdateIdentifier updateId) {
			this.updateId = updateId;
		};
	} // Sent from a replica to the coordinator. ACK for the UpdateRequest

	public static class WriteOkRequest implements Serializable {
		public final UpdateIdentifier updateId;

		WriteOkRequest(UpdateIdentifier updateId) {
			this.updateId = updateId;
		};
	} // Sent from the coordinator to all replicas. Complete the update, modifying the value v

	public static class ElectionRequest implements Serializable { // Sent from a replica to its successor. Initiate a coordinator election
		// TODO: Aggiungere gli update noti da ogni nodo
	}

	public static class ElectionResponse implements Serializable {
	} // Sent from a replica to its predecessor. ACK for the ElectionRequest

	public static class SynchronizationRequest implements Serializable {
	} // Sent to the most updated replica to every other replica. Completes the
		// election, updating the old replicas' v

	public static class HeartbeatMessage implements Serializable {
	} // Sent periodically from the coordinator to all replicas.

	public static class Timeout implements Serializable {
	}

	/*-- Common functionality for both Coordinator and Replicas ------------*/

	public static class Node extends AbstractActor {
		protected int id; // node ID
		protected List<ActorRef> participants; // list of participant nodes
		protected int v; // current v value
		protected Map<UpdateIdentifier, PendingUpdateTuple> pendingUpdates = new HashMap<>(); // updates not yet completed
		protected ArrayList<Update> completedUpdates = new ArrayList<Update>(); // finalized updates
		protected ActorRef coordinator; // current ID of the coordinator node
		protected int e, i; // latest valid values for epoch and update ID

		public Node(int id) {
			super();
			this.id = id;
		}

		void onStartMessage(StartMessage msg) {
			// We assume that the first coordination is the first node of the group
			participants = new ArrayList<>();
			this.coordinator = msg.group.get(0);
			for (ActorRef b : msg.group) {
				this.participants.add(b);
			}
			print("starting with " + msg.group.size() + " peer(s)");
		}

		// emulate a crash and a recovery in a given time
		void crash(int recoverIn) {
			getContext().become(crashed());
			print("CRASH!!!");
		}

		// emulate a delay of d milliseconds
		void delay(int d) {
			try {
				Thread.sleep(d);
			} catch (Exception ignored) {
			}
		}

		void multicast(Serializable m) {
			for (ActorRef p : participants)
				p.tell(m, getSelf());
		}

		// a multicast implementation that crashes after sending the first message
		void multicastAndCrash(Serializable m, int recoverIn) {
			for (ActorRef p : participants) {
				p.tell(m, getSelf());
				crash(recoverIn);
				return;
			}
		}

		// schedule a Timeout message in specified time
		void setTimeout(int time) {
			getContext().system().scheduler().scheduleOnce(Duration.create(time, TimeUnit.MILLISECONDS), getSelf(),
					new Timeout(), // the message to send
					getContext().system().dispatcher(), getSelf());
		}

		void onWrite(IssueWrite msg) {
			if (getSelf().equals(coordinator)) { // If coordinator, forward the uupdate request
				// TODO: Procedere con l'update protocol:
				// Opzione 1: Aggiorno pendingUpdates e invio un UpdateRequest alle replicas -
				// NOP
				// Opzione 2: Invio a tutti (me compreso) l'UpdateRequest - OK
				this.i++;
				UpdateIdentifier identifier = new UpdateIdentifier(e, i);
				print("write " + msg.value + " request recived");
				multicast(new UpdateRequest(new Update(identifier, msg.value)));

			} else { // If replica, forward to the coordinator
				// TODO: Invio un issue al coordinator
				print("redirecting write to coordinator");
				coordinator.tell(new IssueWrite(msg.value), getSender());
			}
		}

		void onRead(IssueRead msg) {
			getSender().tell(v, getSelf());
		}

		// First step of the update protocol, adding a step to the history + sends ACK
		void onUpdateRequest(UpdateRequest msg) {
			PendingUpdateTuple updateList = new PendingUpdateTuple(msg.update.v);
			pendingUpdates.put(msg.update.identifier, updateList);

			print("update request (" + msg.update.identifier.e + ", " + msg.update.identifier.i + ") sending ACK");
			getSender().tell(new UpdateResponse(msg.update.identifier), getSelf());
		}

		// Handle the ACKs. When achieved a quorum, send the writeok
		void onUpdateResponse(UpdateResponse msg) {
			if (getSelf().equals(coordinator)) {
				ArrayList<ActorRef> currentActors = pendingUpdates.get(msg.updateId).actors;
				if (!currentActors.contains(getSender())) { // A node can't vote multiple times
					currentActors.add(getSender());
					print(msg.updateId.toString() + " current quorum = " + currentActors.size());
				}
				if (currentActors.size() == QUORUM) { // If we have reachead the consensus, we send the WriteOK. By using == rather than >=, we avoid sending multiple (useless) WriteOk msgs
					print(msg.updateId.toString() + " quorum reached");
					multicast(new WriteOkRequest(msg.updateId));
				}
			}
		}

		// Complete the update process, changing the value of our v + updating the history
		void onWriteOk(WriteOkRequest msg) {
			// TODO we need to check if the update is the first in the queue before applying the update

			UpdateIdentifier lastUpdate = msg.updateId;
			Update update = new Update(lastUpdate, pendingUpdates.get(lastUpdate).value);
			completedUpdates.add(update);
			pendingUpdates.remove(msg.updateId);

			print("update " + msg.updateId.toString() + " completed, current value " + update.v);

			this.i = update.identifier.i;
			this.v = update.v;
		}

		void onHeartbeatMessage(HeartbeatMessage msg) {
			// Sent periodically from the coordinator to all replicas.
		}

		void onElectionRequest(ElectionRequest msg) {// Sent from a replica to its successor. Initiate a coordinator election
			// TODO: Aggiungere gli update noti da ogni nodo

		}

		void onElectionResponse(ElectionResponse msg) {
			// Sent from a replica to its predecessor. ACK for the ElectionRequest
		}

		// a simple logging function
		void print(String s) {
			String role;
			if (getSelf().equals(coordinator)) {
				role = "Coordinator";
			} else {
				role = "Replica";
			}
			System.out.format("%2d %s: %s\n", id, role, s);
		}

		@Override
		public Receive createReceive() {
			// Empty mapping: we'll define it in the inherited classes
			return receiveBuilder().match(StartMessage.class, this::onStartMessage).match(IssueWrite.class, this::onWrite)
					.match(IssueRead.class, this::onRead).match(HeartbeatMessage.class, this::onHeartbeatMessage)
					.match(ElectionRequest.class, this::onElectionRequest).match(ElectionResponse.class, this::onElectionResponse)
					.match(UpdateRequest.class, this::onUpdateRequest).match(UpdateResponse.class, this::onUpdateResponse)
					.match(WriteOkRequest.class, this::onWriteOk).build();
		}

		public Receive crashed() {
			return null;
			// receiveBuilder()
			// .match(Recovery.class, this::onRecovery)
			// .matchAny(msg -> {})
			// .build();
		}

		static public Props props(int id) {
			return Props.create(Node.class, () -> new Node(id));
		}

	}

	/*-- Main
	------------------------------------------------------------------*/
	public static void main(String[] args) {

		// Create the actor system
		final ActorSystem system = ActorSystem.create("helloakka");

		// Create the coordinator
		// ActorRef coordinator = system.actorOf(Coordinator.props(), "coordinator");

		// Create participants
		List<ActorRef> group = new ArrayList<>();
		for (int i = 0; i < N_PARTICIPANTS; i++) {
			group.add(system.actorOf(Node.props(i), "node" + i));
		}
		// Send start messages to the participants to inform them of the group
		StartMessage start = new StartMessage(group);
		for (ActorRef peer : group) {
			peer.tell(start, null);
		}

		// // Send the start messages to the coordinator
		// coordinator.tell(start, null);
		// group.get(1).tell(new IssueRead(), null);
		group.get(2).tell(new IssueWrite(5), null);
		group.get(0).tell(new IssueWrite(3), null);
		group.get(0).tell(new IssueWrite(10), null);

		try {
			System.out.println(">>> Press ENTER to exit <<<");
			System.in.read();
		} catch (IOException ignored) {
		}
		system.terminate();
	}
}
