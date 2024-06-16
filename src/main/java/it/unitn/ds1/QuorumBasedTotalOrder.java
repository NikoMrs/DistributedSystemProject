package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.japi.Pair;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.HashSet;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Map;
import java.lang.Thread;
import java.util.Collections;
import java.util.HashMap;
import java.io.IOException;
// import java.time.Duration;

public class QuorumBasedTotalOrder {

	// General Configuration
	final static int N_PARTICIPANTS = 4;
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
	final static int REPLICA_TIMEOUT = 2000; // timeout started after sending a msg to a replica. Detects a replica crash
	final static int HEARTHBEAT_FREQUENCY = 10000; // frequency of heartbeat messages
	final static int AVERAGE_RTT = 2000;
	final static int HEARTHBEAT_TIMEOUT = HEARTHBEAT_FREQUENCY + AVERAGE_RTT; // timeout started after receiving a heartbeat msg. Detects a coordinator crash
	final static int DELAY_BOUND = 200;
	// TODO bisogna togliere questo flag e implementare un meccaniscmo per non generare muiltiple elezioni
	static boolean electionStarted = false;

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
		// TODO: Aggiungere gli update noti da ogni nodo Dobbiamo anche aggiungere gli ID dei processi
		public Map<Integer, Pair<ActorRef, UpdateIdentifier>> lastUpdateList = new TreeMap<>();
	}

	public static class ElectionResponse implements Serializable {
	} // Sent from a replica to its predecessor. ACK for the ElectionRequest

	public static class Synchronization implements Serializable {
	} // Sent to the most updated replica to every other replica. Completes the
		// election, updating the old replicas' v

	public static class PreHeartbeatMessage implements Serializable {
	}

	public static class HeartbeatMessage implements Serializable {
	} // Sent periodically from the coordinator to all replicas.

	public static class HeartbeatTimeout implements Serializable {
	}

	public static class Timeout implements Serializable {
	}

	public static class ElectionTimeout implements Serializable {
		ActorRef target;

		ElectionTimeout(ActorRef target) {
			this.target = target;
		}
	}

	/*-- Common functionality for both Coordinator and Replicas ------------*/

	public static class Node extends AbstractActor {
		protected int id; // node ID
		protected List<ActorRef> participants; // list of participant nodes
		protected int v = 0; // current v value
		protected Map<UpdateIdentifier, PendingUpdateTuple> pendingUpdates = new HashMap<>(); // updates not yet completed
		protected ArrayList<Update> completedUpdates = new ArrayList<Update>(); // finalized updates
		protected ActorRef coordinator; // current ID of the coordinator node
		protected int e = 0, i = 0; // latest valid values for epoch and update ID
		protected Cancellable heartBeatTimer, electionTimeout;
		protected Map<UpdateIdentifier, Cancellable> writeOkTimeout = new HashMap<UpdateIdentifier, Cancellable>();

		protected ElectionRequest el_msg = null;
		protected boolean election = false;

		protected Random rnd = new Random();

		public Node(int id) {
			super();
			this.id = id;
			// Adding the firat update
		}

		void onStartMessage(StartMessage msg) {
			// We assume that the first coordination is the first node of the group
			participants = new ArrayList<>();
			this.coordinator = msg.group.get(0);
			// Do if you are the coordinator
			if (this.coordinator.equals(getSelf())) {
				// Set the heartbeat message
				setHeartbeat(HEARTHBEAT_FREQUENCY);
			} else {
				// Set heeartbeat timeout for the replica
				setHeartbeatTimeout(HEARTHBEAT_TIMEOUT);
			}
			for (ActorRef b : msg.group) {
				this.participants.add(b);
			}
			print("starting with " + msg.group.size() + " peer(s)");
		}

		// emulate a crash and a recovery in a given time
		void crash() {
			getContext().become(crashed());
			print("CRASH!!!");
		}

		// enter the election state
		void election() {
			getContext().become(electionMode());
			print("entering ELECTION mode");
		}

		// emulate a delay of d milliseconds
		void delay(int d) {
			try {
				Thread.sleep(d);
			} catch (Exception ignored) {
			}
		}

		void setHeartbeat(int time) {
			getContext().system().scheduler().scheduleWithFixedDelay(Duration.create(time, TimeUnit.MILLISECONDS),
					Duration.create(time, TimeUnit.MILLISECONDS), getSelf(), new PreHeartbeatMessage(),
					getContext().system().dispatcher(), getSelf());
		}

		void setHeartbeatTimeout(int time) {
			this.heartBeatTimer = getContext().system().scheduler().scheduleWithFixedDelay(
					Duration.create(time, TimeUnit.MILLISECONDS), Duration.create(time, TimeUnit.MILLISECONDS), getSelf(),
					new HeartbeatTimeout(), getContext().system().dispatcher(), getSelf());
		}

		void setElectionTimeout(int time, ActorRef target) {
			this.electionTimeout = getContext().system().scheduler().scheduleOnce(
					Duration.create(time, TimeUnit.MILLISECONDS), getSelf(), new ElectionTimeout(target), // the message to send
					getContext().system().dispatcher(), getSelf());
		}

		// void multicast(Serializable m) {
		// for (ActorRef p : participants)
		// p.tell(m, getSelf());
		// }

		void multicast(Serializable m) {
			// randomly arrange peers
			List<ActorRef> shuffledGroup = new ArrayList<>(participants);
			Collections.shuffle(shuffledGroup);

			// this.multicastTimout.clear();

			// We need to first send the message to the coordinator without ay delay, and after send to the others with a random delay
			this.getSelf().tell(m, this.getSelf());

			// multicast to all peers in the group (do not send any message to self)
			for (ActorRef p : shuffledGroup) {
				if (!p.equals(getSelf())) {
					p.tell(m, getSelf());
					// simulate network delays using sleep
					delay(DELAY_BOUND);
				}
			}
			crash();
		}

		// a multicast implementation that crashes after sending the first message
		void multicastAndCrash(Serializable m, int recoverIn) {

			// randomly arrange peers
			List<ActorRef> shuffledGroup = new ArrayList<>(participants);
			Collections.shuffle(shuffledGroup);

			// We need to first send the message to the coordinator without ay delay, and after send to the others with a random delay
			getSelf().tell(m, getSelf());

			// multicast to all peers in the group (do not send any message to self)
			for (ActorRef p : shuffledGroup) {
				if (!p.equals(getSelf())) {
					p.tell(m, getSelf());

					// Send one msg and crash
					crash();
					return;
				}
			}

			for (ActorRef p : participants) {
				p.tell(m, getSelf());

			}
		}

		// schedule a Timeout message in specified time
		Cancellable setTimeout(int time) {
			return getContext().system().scheduler().scheduleOnce(Duration.create(time, TimeUnit.MILLISECONDS), getSelf(),
					new Timeout(), // the message to send
					getContext().system().dispatcher(), getSelf());
		}

		void onWrite(IssueWrite msg) {
			if (getSelf().equals(coordinator)) { // If coordinator, forward the uupdate request
				this.i++;
				UpdateIdentifier identifier = new UpdateIdentifier(e, i);
				print("write " + msg.value + " request recived");
				multicast(new UpdateRequest(new Update(identifier, msg.value)));

			} else { // If replica, forward to the coordinator
				print("redirecting write to coordinator");
				delay(DELAY_BOUND); // Simulating networtk delay
				coordinator.tell(new IssueWrite(msg.value), getSender());
				// TODO aggiungere e gestire un timeout in caso il coordinatore non inizi un update
			}
		}

		void onRead(IssueRead msg) {
			getSender().tell(v, getSelf());
		}

		// First step of the update protocol, adding a step to the history + sends ACK
		void onUpdateRequest(UpdateRequest msg) {
			PendingUpdateTuple updateList = new PendingUpdateTuple(msg.update.v);
			pendingUpdates.put(msg.update.identifier, updateList);

			if (!(getSelf().equals(this.coordinator))) {
				delay(DELAY_BOUND); // network delay
			}

			print("update request (" + msg.update.identifier.e + ", " + msg.update.identifier.i + ") sending ACK");

			getSender().tell(new UpdateResponse(msg.update.identifier), getSelf());

			// Set the WritrOk timeout
			this.writeOkTimeout.put(msg.update.identifier, setTimeout(WRITEOK_TIMEOUT));
		}

		// Handle the ACKs. When achieved a quorum, send the writeok
		void onUpdateResponse(UpdateResponse msg) {
			if (getSelf().equals(coordinator)) {
				// print(pendingUpdates + " - " + msg.updateId);
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
			// We need to cancel the corresponding timeout
			this.writeOkTimeout.get(msg.updateId).cancel();
			this.writeOkTimeout.remove(msg.updateId);

			UpdateIdentifier lastUpdate = msg.updateId;
			Update update = new Update(lastUpdate, pendingUpdates.get(lastUpdate).value);
			completedUpdates.add(update);
			pendingUpdates.remove(msg.updateId);

			print("update " + msg.updateId.toString() + " completed, current value " + update.v);

			this.i = update.identifier.i;
			this.v = update.v;
		}

		void onPreHeartbeatMessage(PreHeartbeatMessage msg) {
			// Sent periodically from the coordinator to all replicas.
			print("Sending Heartbeat");
			multicast(new HeartbeatMessage());
		}

		void onHeartbeatMessage(HeartbeatMessage msg) {
			// Sent periodically from the coordinator to all replicas.
			if (!this.coordinator.equals(getSelf())) {
				this.heartBeatTimer.cancel();
				print("coordinator alive");
			}
		}

		void onHeartbeatTimeout(HeartbeatTimeout msg) {
			print("coordinator crashed, starting new election...");
			// TODO scrivo al primo nodo dell'anello e e poi lui contatta tutti gli altri membri in broadcast avvisando dell'inizio di una nuova elezione (timuovere l'if)
			if (!electionStarted) {
				// TODO bisogna ruimuovere questa variabile
				electionStarted = true;

				if (!election) {
					election = true;
					election();
				}
				ActorRef next = this.participants.get((this.participants.indexOf(getSelf()) + 1) % (this.participants.size()));
				el_msg = new ElectionRequest();
				el_msg.lastUpdateList.put(this.id, new Pair<>(getSelf(), new UpdateIdentifier(this.e, this.i)));
				next.tell(el_msg, getSelf());
				// TODO aggiungere un timeout che tiene traccia del nodo al quale abbiamo inviato il messaggio e in caso scatti lo rimuova dall'anello e reinvii Election msg
				setElectionTimeout(500, next);
			}

		}

		void onElectionRequest(ElectionRequest msg) {// Sent from a replica to its successor. Initiate a coordinator election
			// TODO: Aggiungere gli update noti da ogni nodo
			// print(msg.lastUpdateList + "");
			print("Election request recived");
			if (!election) {
				election = true;
				election();
			}
			getSender().tell(new ElectionResponse(), getSelf());
			// TODO bisogna mandare il messaggio di Election request anche al nodo successivo
			el_msg = msg;
			if (el_msg.lastUpdateList.get(this.id) == null) {
				el_msg.lastUpdateList.put(this.id, new Pair<>(getSelf(), new UpdateIdentifier(this.e, this.i)));
				// I need to find the next node in the link and send forward the election message
				// TODO riformulare con una funzione piu' elegante
				ActorRef next = this.participants.get((this.participants.indexOf(getSelf()) + 1) % (this.participants.size()));
				next.tell(el_msg, getSelf());
				setElectionTimeout(500, next);
			} else {
				// TODO siccome il mio ultimo update e' gia' presente devo eleggere il coordinatore e propagare l'informazione, si potrebbe propagare anche la nuova topologia dell'anello (opzionale)
				List<ActorRef> newParticipants = new ArrayList<>();
				Pair<ActorRef, UpdateIdentifier> last = null;
				int lastId = -1;

				for (Map.Entry<Integer, Pair<ActorRef, UpdateIdentifier>> entry : el_msg.lastUpdateList.entrySet()) {
					Integer key = entry.getKey();
					Pair<ActorRef, UpdateIdentifier> value = entry.getValue();
					// In this way they are already ordered by id
					newParticipants.add(value.first());

					if (last == null) {
						last = value;
						lastId = key;
						continue;
					}

					if (value.second().e == last.second().e && value.second().i > last.second().i) {
						last = value;
						lastId = key;
					} else if (value.second().e > last.second().e) {
						last = value;
						lastId = key;
					} else if (value.second().e == last.second().e && value.second().i == last.second().i && key > lastId) {
						last = value;
						lastId = key;
					}
				}
				// if i'm not the new coordinator just forward the Election message
				if (!(this.id == lastId)) {
					// TODO bisogna snellire il codice
					ActorRef next = this.participants
							.get((this.participants.indexOf(getSelf()) + 1) % (this.participants.size()));
					next.tell(el_msg, getSelf());
					setElectionTimeout(500, next);
				} else {
					// TODO inviare un sincronization message a tutti i nodi attivi
					print("SYNC (" + this.id + ")");
					// Create a new partecipants list
					this.participants = newParticipants;
					this.coordinator = last.first();
					multicast(new Synchronization());
				}
			}
		}

		void onElectionResponse(ElectionResponse msg) {
			// Sent from a replica to its predecessor. ACK for the ElectionRequest
			print("ELECTION ACK RECIVED");
			// Ack arrived timeout cancelled
			this.electionTimeout.cancel();
		}

		void onElectionTimeout(ElectionTimeout msg) {
			this.participants.remove(msg.target);
			print("REMOVED");
			// TODO inviare un nuovo messaggio al nodo successivo visto che abbiamo rimosso quello in crash
			print("ELECTION TIMEOUT");
			ActorRef next = this.participants.get((this.participants.indexOf(getSelf()) + 1) % (this.participants.size()));
			// If we don't have an election message we have to create a new one
			if (el_msg == null) {
				el_msg = new ElectionRequest();
				el_msg.lastUpdateList.put(this.id, new Pair<>(getSelf(), new UpdateIdentifier(this.e, this.i)));
			}
			next.tell(el_msg, getSelf());
			// TODO aggiungere un timeout che tiene traccia del nodo al quale abbiamo inviato il messaggio e in caso scatti lo rimuova dall'anello e reinvii Election msg
			setElectionTimeout(500, next);

		}

		void onSynchronization(Synchronization msg) {
			print("Sync recived by coordinator");
			// TODO completare la sincronizzaizone
			this.coordinator = getSender(); // Change coordinator ref
			// Update partecipants
			// Synchronize to last update
			// Exit election mode

		}

		void onWriteOkTimeout(Timeout msg) {
			print("coordinator crashed on WriteOk, starting new election...");
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
					.match(ElectionRequest.class, this::onElectionRequest).match(UpdateRequest.class, this::onUpdateRequest)
					.match(UpdateResponse.class, this::onUpdateResponse).match(WriteOkRequest.class, this::onWriteOk)
					.match(PreHeartbeatMessage.class, this::onPreHeartbeatMessage)
					.match(HeartbeatTimeout.class, this::onHeartbeatTimeout).match(Timeout.class, this::onWriteOkTimeout).build();
		}

		public Receive crashed() {
			return receiveBuilder().matchAny(msg -> {
			}).build();
		}

		public Receive electionMode() {
			return receiveBuilder().match(ElectionRequest.class, this::onElectionRequest)
					.match(ElectionResponse.class, this::onElectionResponse).match(Synchronization.class, this::onSynchronization)
					.match(ElectionTimeout.class, this::onElectionTimeout).matchAny(msg -> {
					}).build();
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
		// group.get(0).tell(new IssueWrite(3), null);
		// group.get(0).tell(new IssueWrite(10), null);

		try {
			System.out.println(">>> Press ENTER to exit <<<");
			System.in.read();
		} catch (IOException ignored) {
		}
		system.terminate();
	}
}
