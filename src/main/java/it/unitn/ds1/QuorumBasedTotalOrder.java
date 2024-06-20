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
import java.util.Map.Entry;
import java.lang.Thread;
import java.util.Collections;
import java.util.HashMap;
import java.io.IOException;
// import java.time.Duration;

public class QuorumBasedTotalOrder {

	// General Configuration
	final static int N_PARTICIPANTS = 5;
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
	final static int INIT_TIMEOUT = 2000;
	final static int SYNC_TIMEOUT = 20000;
	// TODO bisogna togliere questo flag e implementare un meccaniscmo per non generare muiltiple elezioni
	static boolean electionStarted = false;
	static boolean crash = true;

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
		public Map<Integer, Pair<ActorRef, UpdateIdentifier>> lastUpdateList = new TreeMap<>();
	}

	public static class ElectionResponse implements Serializable {
	} // Sent from a replica to its predecessor. ACK for the ElectionRequest

	public static class Synchronization implements Serializable {
		List<ActorRef> participants;
		ArrayList<Update> completedUpdates;

		Synchronization(List<ActorRef> participants, ArrayList<Update> completedUpdates) {
			this.participants = participants;
			this.completedUpdates = completedUpdates;
		}
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

	public static class ElectionInit implements Serializable { // Used to contact the first active replica in the ring, asking to start the election process.
	}

	public static class ElectionInitAck implements Serializable {
	}

	public static class ElectioIninitTimeout implements Serializable {
		ActorRef target;

		ElectioIninitTimeout(ActorRef target) {
			this.target = target;
		}
	}

	public static class ElectionStartTimeout implements Serializable {
	}

	public static class ElectionCompletedTimeout implements Serializable {
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
		protected Cancellable heartBeatTimer, electionTimeout, initTimeout, electionStart, electionCompleted;
		protected Map<UpdateIdentifier, Cancellable> writeOkTimeout = new HashMap<UpdateIdentifier, Cancellable>();
		protected boolean initStarted = false;

		protected ElectionRequest el_msg = null;
		protected boolean election = false;
		// TODO Se siamo in election mode (election = true) e riceviamo degli election inti, rispondiamo solo con un ACK, senza iniziare una nuova elezione

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
				// setHeartbeatTimeout(HEARTHBEAT_TIMEOUT);
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
			getContext().system().scheduler().scheduleWithFixedDelay(Duration.create(0, TimeUnit.MILLISECONDS),
					Duration.create(time, TimeUnit.MILLISECONDS), getSelf(), new PreHeartbeatMessage(),
					getContext().system().dispatcher(), getSelf());
		}

		void setHeartbeatTimeout(int time) {
			this.heartBeatTimer = getContext().system().scheduler().scheduleOnce(Duration.create(time, TimeUnit.MILLISECONDS),
					getSelf(), new HeartbeatTimeout(), getContext().system().dispatcher(), getSelf());
		}

		void setElectionTimeout(int time, ActorRef target) {
			this.electionTimeout = getContext().system().scheduler().scheduleOnce(
					Duration.create(time, TimeUnit.MILLISECONDS), getSelf(), new ElectionTimeout(target), // the message to send
					getContext().system().dispatcher(), getSelf());
		}

		void setElectioIninitTimeout(int time, ActorRef target) {
			this.initTimeout = getContext().system().scheduler().scheduleOnce(Duration.create(time, TimeUnit.MILLISECONDS),
					getSelf(), new ElectioIninitTimeout(target), // the message to send
					getContext().system().dispatcher(), getSelf());
		}

		void setElectionStartTimeout(int time) {
			this.electionStart = getContext().system().scheduler().scheduleOnce(Duration.create(time, TimeUnit.MILLISECONDS),
					getSelf(), new ElectionStartTimeout(), // the message to send
					getContext().system().dispatcher(), getSelf());
		}

		void setElectionCompletedTimeout(int time) {
			this.electionCompleted = getContext().system().scheduler().scheduleOnce(
					Duration.create(time, TimeUnit.MILLISECONDS), getSelf(), new ElectionCompletedTimeout(), // the message to send
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
			int i = 1;

			for (ActorRef p : shuffledGroup) {
				i++;
				if (i == N_PARTICIPANTS && crash) {
					crash = false;
					// crash();
					// break;
				}
				if (!p.equals(getSelf())) {
					p.tell(m, getSelf());
					// simulate network delays using sleep
					delay(DELAY_BOUND);
				}
			}

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
					// TODO remove the crash
					if (this.id == 0)
						crash();
					else {
						multicast(new WriteOkRequest(msg.updateId));
					}
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

			// TODO modificare, da problemi
			if (!this.election)
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
				if (this.heartBeatTimer != null)
					this.heartBeatTimer.cancel();
				print("coordinator alive");
				// Set next timeout
				setHeartbeatTimeout(HEARTHBEAT_TIMEOUT);
			}
		}

		void onHeartbeatTimeout(HeartbeatTimeout msg) {
			print("coordinator crashed, election Init...");
			// TODO Rimuovere l'if
			// if (!electionStarted) {
			// TODO bisogna rimuovere questa variabile
			electionStarted = true;

			if (!election) {
				election = true;
				election();
			}

			// TODO effettuare l'election init se non siamo gia' in election mode (dobbiamo gestire l'anello per individuare il primo nodo disponibile)
			// troviamo il primo nodo attivo dell'anello e cerchiamo di contattarlo con un electionInit.
			// if (!this.election) { // Potrebbe non essere necessario il controllo
			// // Get the first active node in the topology (the lower id)
			// ActorRef first = this.participants.get(0);
			// first.tell(new ElectionInit(), getSelf());
			// setElectioIninitTimeout(INIT_TIMEOUT, first);
			// }
			// Try to contact the first active node of the ring and set a timout
			ActorRef first = this.participants.get(0);
			first.tell(new ElectionInit(), getSelf());
			setElectioIninitTimeout(INIT_TIMEOUT, first);
			// }

		}

		// TODO Aggiungere Election Init, inviato al primo nodo del ring. Fa partire un timeout. Se scade prima di ricevere un ACK, comunica con il nodo
		// successivo (freccia 1)
		// TODO Quando ricevo l'ACK dell'election Init (2), rimuovo il timeout prec e ne avvio un'altro. Se scade prima di ricevere un election msg (quindi
		// non è stata avviata l'elezione), faccio partire una nuova elezione
		// TODO Una volta ricevuto un election msg (3), resetto il timer sopra. Se scade prima di ricevere un synchronization msg (quindi è crashato il
		// coordinator eletto), avvio una nuova elezione
		void onElectionRequest(ElectionRequest msg) {// Sent from a replica to its successor. Initiate a coordinator election
			// print(msg.lastUpdateList + "");

			// TODO cancello l'init timeout + ne avvio uno per il completamento della election (SYNC). se scade significa che il coordinatore e' crashato
			// ulteriormente ed effettuo quindi un altro Init
			if (this.electionStart != null)
				this.electionStart.cancel();
			if (this.initTimeout != null)
				this.initTimeout.cancel();

			// Set initstarter to true (prevent multiple elections)
			this.initStarted = true;

			print("Election request recived");
			if (!election) {
				election = true;
				election();
			}
			getSender().tell(new ElectionResponse(), getSelf());
			el_msg = msg;
			if (el_msg.lastUpdateList.get(this.id) == null) {
				// TODO settiamo un timer per verificare che l'elezione termini lo setto solo la prima volta che ricevo l'election request
				setElectionCompletedTimeout(SYNC_TIMEOUT); // Set SYNC timeout, the procedure need to terminate or restart

				el_msg.lastUpdateList.put(this.id, new Pair<>(getSelf(), new UpdateIdentifier(this.e, this.i)));
				// I need to find the next node in the link and send forward the election message
				// TODO riformulare con una funzione piu' elegante
				ActorRef next = this.participants.get((this.participants.indexOf(getSelf()) + 1) % (this.participants.size()));
				next.tell(el_msg, getSelf());
				setElectionTimeout(500, next);
			} else {
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
					// TODO Faccio partire un timeout. In caso questo scada prima che venga ricevuto un synchronization msg, cioè se qualcosa è andato storto durante
					// l'election (crash del nuovo coordinator), inizio nuovamente l'elezione
					ActorRef next = this.participants
							.get((this.participants.indexOf(getSelf()) + 1) % (this.participants.size()));
					next.tell(el_msg, getSelf());
					setElectionTimeout(500, next);
				} else {
					print("SYNC (" + this.id + ")");
					// Create a new partecipants list
					this.participants = newParticipants;
					multicast(new Synchronization(this.participants, this.completedUpdates));

					// Sending pending updates
					if (!this.pendingUpdates.isEmpty())
						print("Sending pending update...");
					for (Map.Entry<UpdateIdentifier, PendingUpdateTuple> entry : this.pendingUpdates.entrySet()) {
						print(entry.getKey() + " - " + entry.getValue());
						// multicast
						Update update = new Update(entry.getKey(), entry.getValue().value);
						multicast(new UpdateRequest(update));
					}
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
			print("ELECTION TIMEOUT");
			print("REMOVED");
			ActorRef next = this.participants.get((this.participants.indexOf(getSelf()) + 1) % (this.participants.size()));
			// If we don't have an election message we have to create a new one
			if (el_msg == null) {
				el_msg = new ElectionRequest();
				el_msg.lastUpdateList.put(this.id, new Pair<>(getSelf(), new UpdateIdentifier(this.e, this.i)));
			}
			next.tell(el_msg, getSelf());
			setElectionTimeout(500, next);

		}

		void onElectionInitTimeout(ElectioIninitTimeout msg) {
			this.participants.remove(msg.target);
			print("ELECTION INIT TIMEOUT");
			print("REMOVED");
			ActorRef first = this.participants.get(0);
			// If we don't have an election message we have to create a new one
			first.tell(new ElectionInit(), getSelf());
			setElectioIninitTimeout(INIT_TIMEOUT, first);
		}

		void onElectionStartTimeout(ElectionStartTimeout msg) {
			// The election is not started, the node that started the election (Election Init), crashed before starting the election
			// Just send another Election Init
			ActorRef first = this.participants.get(0);
			first.tell(new ElectionInit(), getSelf());
			setElectioIninitTimeout(INIT_TIMEOUT, first);
		}

		void onElectionCompletedTimeout(ElectionCompletedTimeout msg) {
			// The election didn't complete, probably the new coordinator crashed, starting another election

			// reset initStarted
			this.initStarted = false;

			ActorRef first = this.participants.get(0);
			first.tell(new ElectionInit(), getSelf());
			setElectioIninitTimeout(INIT_TIMEOUT, first);
		}

		void onSynchronization(Synchronization msg) {
			// TODO cancellare il synchronization timeout (Scatta quando non dovrebbe)
			if (this.electionCompleted != null)
				this.electionCompleted.cancel();
			print("Sync recived by coordinator");
			this.coordinator = getSender(); // Change coordinator ref
			this.participants = msg.participants; // Update partecipants
			this.completedUpdates = msg.completedUpdates; // Synchronize to last update

			// reset initStarted
			this.initStarted = false;

			if (!msg.completedUpdates.isEmpty()) {
				// There are completed updates
				Update update = msg.completedUpdates.get(msg.completedUpdates.size());
				this.v = update.v;
				this.e = update.identifier.e;
			}
			this.e++;
			this.i = 0;
			// Exit election mode
			getContext().become(createReceive());

			// Reschedule the heartbeat msg
			if (!getSelf().equals(this.coordinator)) {
				// If i'm not the coordinator empty the pending updates (we will get them from the coordinator)
				this.pendingUpdates.clear();
				setHeartbeatTimeout(HEARTHBEAT_TIMEOUT);
			} else {
				setHeartbeat(HEARTHBEAT_FREQUENCY);
			}
			print("Last Update: (" + this.e + ", " + this.i + ") = " + this.v);
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

		void onElectionInit(ElectionInit msg) {
			// risponde con un ack + enter election mode
			// TODO controllare che se sono gia' in election ....
			if (!this.election) {
				this.election = true;
				election();
			}
			// se sono gia' in election mode rispondo solo
			print("Sending Election Init ACK...");
			getSender().tell(new ElectionInitAck(), getSelf());
			// avvio la procedura di election

			// TODO rimuovere l'if per togliere il crash
			// if (this.id == 1) {
			// crash();
			// } else {
			// Start the ring election process
			if (this.initStarted == false) {
				print("STARTING ELECTION");
				this.initStarted = true;
				ActorRef next = this.participants.get((this.participants.indexOf(getSelf()) + 1) % (this.participants.size()));
				el_msg = new ElectionRequest();
				el_msg.lastUpdateList.put(this.id, new Pair<>(getSelf(), new UpdateIdentifier(this.e, this.i)));
				next.tell(el_msg, getSelf());
				setElectionTimeout(500, next);
				if (this.id == 1)
					crash();
			}
			// }

		}

		void onElectionInitAck(ElectionInitAck msg) {
			// cancelliamo il timer per ElectionInit
			this.initTimeout.cancel();
			// setto un ulteriore timer che aspetta l'arrivo di un election message (significa che l'election sta procedendo)
			setElectionStartTimeout(2000); // se non arriva invio un'altro election init
		}

		@Override
		public Receive createReceive() {
			// Empty mapping: we'll define it in the inherited classes
			return receiveBuilder().match(StartMessage.class, this::onStartMessage).match(IssueWrite.class, this::onWrite)
					.match(IssueRead.class, this::onRead).match(HeartbeatMessage.class, this::onHeartbeatMessage)
					.match(ElectionRequest.class, this::onElectionRequest).match(UpdateRequest.class, this::onUpdateRequest)
					.match(UpdateResponse.class, this::onUpdateResponse).match(WriteOkRequest.class, this::onWriteOk)
					.match(PreHeartbeatMessage.class, this::onPreHeartbeatMessage).match(ElectionInit.class, this::onElectionInit)
					.match(ElectionInitAck.class, this::onElectionInitAck).match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
					.match(Timeout.class, this::onWriteOkTimeout).match(ElectioIninitTimeout.class, this::onElectionInitTimeout)
					.build();
		}

		public Receive crashed() {
			return receiveBuilder().matchAny(msg -> {
			}).build();
		}

		public Receive electionMode() {
			return receiveBuilder().match(ElectionRequest.class, this::onElectionRequest)
					.match(ElectionResponse.class, this::onElectionResponse).match(Synchronization.class, this::onSynchronization)
					.match(ElectionTimeout.class, this::onElectionTimeout).match(ElectionInitAck.class, this::onElectionInitAck)
					.match(ElectionInit.class, this::onElectionInit)
					.match(ElectioIninitTimeout.class, this::onElectionInitTimeout)
					.match(ElectionCompletedTimeout.class, this::onElectionCompletedTimeout).matchAny(msg -> {
					}).build();
		}

		static public Props props(int id) {
			return Props.create(Node.class, () -> new Node(id));
		}

	}

	/*-- Main
	------------------------------------------------------------------*/
	public static void main(String[] args) throws InterruptedException {

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
		Thread.sleep(20000);
		group.get(2).tell(new IssueWrite(10), null);
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
