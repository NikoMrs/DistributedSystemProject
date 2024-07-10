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
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.lang.Thread;
import java.util.Collections;
import java.util.HashMap;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File;
import java.io.FileWriter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class QuorumBasedTotalOrder {

	// Log Configuration
	final static String LOG_DIRECTORY = "./logs/";
	public static String log_filename;

	// General Configuration
	final static int N_PARTICIPANTS = 5;
	final static int QUORUM = (N_PARTICIPANTS / 2) + 1;

	public static class UpdateIdentifier {
		public int e; // Epoch identifier
		public int i; // In-epoch identifier

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

	// Used to hold informations about the value and the votes received for an Update
	public static class PendingUpdateTuple {
		ArrayList<ActorRef> actors;
		int value;

		public PendingUpdateTuple(int value) {
			this.value = value;
			actors = new ArrayList<>();
		}
	}

	// Operations alloweds
	public enum Operation {
		READ, WRITE
	}

	// Timeouts Configuration
	final static int WRITEOK_TIMEOUT = 3000; // Timeout started after receiving the update msg. Detects a coordinator crash
	final static int BROADCAST_INITIATE_TIMEOUT = 1000; // Timeout started after sending a write request. Detects a coordinator crash
	final static int HEARTHBEAT_FREQUENCY = 10000; // Frequency of heartbeat messages
	final static int MAX_RTT = 2000;
	final static int HEARTHBEAT_TIMEOUT = HEARTHBEAT_FREQUENCY + MAX_RTT; // Timeout started after receiving a heartbeat msg. Detects a coordinator crash
	final static int DELAY_BOUND = 200; // Max amount of time allowed for a generated delay
	final static int INIT_TIMEOUT = 2000; // Timeout started after sending an ElectionInit msg. Detects a not-started election, trough the missing reception of an ElectionInitAck
	final static int START_TIMEOUT = 2000; // Timeout started after sending an ElectionInit msg. Detects a not-started election, trough the missing reception of an ElectionRequest
	final static int SYNC_TIMEOUT = 20000; // Timeout started after receiving an ElectionRequest msg for the first time. Used to guarantee a termination of the election
	final static int ISSUE_TIMEOUT = 20000; // Timeout started after forwarding an IssueWrite to the coordinator. Detects a coordinator crash
	final static int ELECTION_TIMEOUT = 500; // Timeout started afer sending an ElectionRequest. Detects a node crash

	// static boolean crash = true;

	// Messages Configuration
	public static class StartMessage implements Serializable { // Start message that sends the list of participants to everyone
		public final List<ActorRef> group;

		public StartMessage(List<ActorRef> group) {
			this.group = Collections.unmodifiableList(new ArrayList<>(group));
		}
	}

	public static class IssueWrite implements Serializable { // Sent from a client to a replica or from a replica to the coordinator. Asks for a Write operation, specifying the value to write
		public int value;

		IssueWrite(int value) {
			this.value = value;
		};
	}

	public static class IssueRead implements Serializable { // Sent from a client to a replica. Asks for a Read operation
	}

	public static class UpdateRequest implements Serializable { // Sent from the coordinator to all replicas. Initiate the update protocol
		public final Update update;
		public final ActorRef caller;

		UpdateRequest(Update update, ActorRef caller) {
			this.update = update;
			this.caller = caller;
		};
	}

	public static class UpdateResponse implements Serializable { // Sent from a replica to the coordinator. ACK for the UpdateRequest
		public final UpdateIdentifier updateId; // Update identifier <e,i>. Used to specify to which update we are referring to

		UpdateResponse(UpdateIdentifier updateId) {
			this.updateId = updateId;
		};
	}

	public static class WriteOkRequest implements Serializable { // Sent from the coordinator to all replicas. Complete the update, modifying the value v
		public final UpdateIdentifier updateId; // Update identifier <e,i>. Used to specify to which update we are referring to

		WriteOkRequest(UpdateIdentifier updateId) {
			this.updateId = updateId;
		};
	}

	public static class ElectionRequest implements Serializable { // Sent from a replica to its successor. Used for a coordinator election
		public Map<Integer, Pair<ActorRef, UpdateIdentifier>> lastUpdateList = new TreeMap<>(); // Used to keep track of the latest update of each node
	}

	public static class ElectionResponse implements Serializable { // Sent from a replica to its predecessor. ACK for the ElectionRequest
	}

	public static class Synchronization implements Serializable { // Sent from the most updated replica to every other replica. Completes the election
		List<ActorRef> participants; // List of current active replicas
		ArrayList<Update> completedUpdates; // List of already completed updates

		Synchronization(List<ActorRef> participants, ArrayList<Update> completedUpdates) {
			this.participants = participants;
			this.completedUpdates = completedUpdates;
		}
	}

	public static class PreHeartbeatMessage implements Serializable { // Sent periodically to the coordinator from itself. Used to schedule the Heartbeat
	}

	public static class HeartbeatMessage implements Serializable { // Sent periodically from the coordinator to all replicas.
	}

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

	public static class ElectionInit implements Serializable { // Sent to the first active replica in the ring. Used to start the election process.
	}

	public static class ElectionInitAck implements Serializable { // Sent from a node to another. Used as ACK for the ElectionInit
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
		protected int id; // Node ID
		protected List<ActorRef> participants; // List of participant nodes
		protected int v = 0; // Current v value
		protected Map<UpdateIdentifier, PendingUpdateTuple> pendingUpdates = new HashMap<>(); // Updates not yet completed
		protected ArrayList<Update> completedUpdates = new ArrayList<Update>(); // Finalized updates
		protected ActorRef coordinator; // Current ID of the coordinator node
		protected int e = 0, i = 0; // Latest valid values for epoch and update ID

		// Timeouts
		protected Cancellable heartBeatTimer, electionTimeout, initTimeout, electionStartTimeout, electionCompletedTimeout;
		protected ArrayList<Cancellable> forwardingTimeoutList = new ArrayList<Cancellable>();
		protected Map<UpdateIdentifier, Cancellable> writeOkTimeout = new HashMap<UpdateIdentifier, Cancellable>();

		protected boolean initStarted = false; // Used to check if there is already an election message going through the ring
		protected boolean election = false; // Used to check if we are already in election mode
		protected ElectionRequest el_msg = null;

		protected Random rnd = new Random();

		public Node(int id) {
			super();
			this.id = id;
		}

		void onStartMessage(StartMessage msg) {
			// We assume that the first coordination is the first node of the group
			participants = new ArrayList<>();
			this.coordinator = msg.group.get(0);

			// Do if you are the coordinator
			if (this.coordinator.equals(getSelf())) {
				// Set the heartbeat message
				setHeartbeat(HEARTHBEAT_FREQUENCY);
			}

			for (ActorRef b : msg.group) {
				this.participants.add(b);
			}
			print("Starting with " + msg.group.size() + " peer(s)");
		}

		ActorRef getNext() { // Used to get the next node in the ring
			return this.participants.get((this.participants.indexOf(getSelf()) + 1) % (this.participants.size()));
		}

		ActorRef getFirst() { // Used to get the first node in the ring
			return this.participants.get(0);
		}

		void crash() { // Emulates a crash and a recovery in a given time
			getContext().become(crashed());
			print("CRASH!!!");
		}

		void election() { // Used to enter in the election mode
			getContext().become(electionMode());
			print("Entering ELECTION mode");
		}

		void delay(int d) { // Emulates a delay of d milliseconds
			try {
				Thread.sleep(d);
			} catch (Exception ignored) {
			}
		}

		// Timeouts handling
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

		void setElectionInitTimeout(int time, ActorRef target) {
			this.initTimeout = getContext().system().scheduler().scheduleOnce(Duration.create(time, TimeUnit.MILLISECONDS),
					getSelf(), new ElectioIninitTimeout(target), // the message to send
					getContext().system().dispatcher(), getSelf());
		}

		void setElectionStartTimeout(int time) {
			this.electionStartTimeout = getContext().system().scheduler().scheduleOnce(
					Duration.create(time, TimeUnit.MILLISECONDS), getSelf(), new ElectionStartTimeout(), // the message to send
					getContext().system().dispatcher(), getSelf());
		}

		void setElectionCompletedTimeout(int time) {
			this.electionCompletedTimeout = getContext().system().scheduler().scheduleOnce(
					Duration.create(time, TimeUnit.MILLISECONDS), getSelf(), new ElectionCompletedTimeout(), // the message to send
					getContext().system().dispatcher(), getSelf());
		}

		Cancellable setTimeout(int time) { // Schedule a Timeout message in specified time
			return getContext().system().scheduler().scheduleOnce(Duration.create(time, TimeUnit.MILLISECONDS), getSelf(),
					new Timeout(), // the message to send
					getContext().system().dispatcher(), getSelf());
		}

		void multicast(Serializable m) { // Multicast a message to the whole network
			// Randomly arrange peers
			List<ActorRef> shuffledGroup = new ArrayList<>(participants);
			Collections.shuffle(shuffledGroup);

			// The node calling multicast will send the message to itself without delay
			this.getSelf().tell(m, this.getSelf());

			// Multicast to all peers in the group (do not send any message to self)

			// int i = 1;
			for (ActorRef p : shuffledGroup) {
				// i++;
				// if (i == N_PARTICIPANTS && crash) {
				// crash = false;
				// // crash();
				// // break;
				// }
				if (!p.equals(getSelf())) {
					p.tell(m, getSelf());
					// Simulate network delays using sleep
					delay(rnd.nextInt(DELAY_BOUND));
				}
			}

		}

		void onWrite(IssueWrite msg) { // Write request handler
			if (getSelf().equals(coordinator)) { // If coordinator, forward the update request
				this.i++;
				UpdateIdentifier identifier = new UpdateIdentifier(e, i);
				UpdateRequest uReq;

				if (getSender() == null) // If received from a client
					uReq = new UpdateRequest(new Update(identifier, msg.value), getSelf());
				else
					uReq = new UpdateRequest(new Update(identifier, msg.value), getSender());

				print("Write " + msg.value + " request recived");
				multicast(uReq);

			} else { // If replica, forward the request to the coordinator
				print("Redirecting write " + msg.value + " to coordinator");

				delay(rnd.nextInt(DELAY_BOUND)); // Simulating networtk delay

				coordinator.tell(new IssueWrite(msg.value), getSelf());
				this.forwardingTimeoutList.add(setTimeout(ISSUE_TIMEOUT)); // Add a timeout for the write operation
			}
		}

		void onRead(IssueRead msg) { // Read request handler
			delay(rnd.nextInt(DELAY_BOUND));
			getSender().tell(v, getSelf());
			print("Value " + v + " read from the replica");
		}

		void onUpdateRequest(UpdateRequest msg) { // First step of the update protocol, adding a step to the history + sends ACK
			PendingUpdateTuple updateList = new PendingUpdateTuple(msg.update.v);
			pendingUpdates.put(msg.update.identifier, updateList); // Add the updated to the pending list

			// If I've forwarded the write, cancel the timeout
			if (getSelf().equals(msg.caller) && this.forwardingTimeoutList.size() != 0) {
				print("Issue Delay removed");
				this.forwardingTimeoutList.get(0).cancel();
				this.forwardingTimeoutList.remove(0);
			}

			print("Update request (" + msg.update.identifier.e + ", " + msg.update.identifier.i + ") from " + getSender()
					+ ". Sending ACK");

			getSender().tell(new UpdateResponse(msg.update.identifier), getSelf()); // Send ACK

			this.writeOkTimeout.put(msg.update.identifier, setTimeout(WRITEOK_TIMEOUT)); // Set the WriteOk timeout
		}

		void onUpdateResponse(UpdateResponse msg) { // Handle the ACKs for UpdateRequest. When achieved a quorum, send the WriteOk
			if (getSelf().equals(coordinator)) {
				ArrayList<ActorRef> currentActors = pendingUpdates.get(msg.updateId).actors;

				if (!currentActors.contains(getSender())) { // A node can't vote multiple times
					currentActors.add(getSender());
					print("ACK for " + msg.updateId.toString() + " from " + getSender() + ", current quorum = "
							+ currentActors.size());
				}

				if (currentActors.size() == QUORUM) { // If we have reachead the consensus, we send the WriteOK. By using == rather than >=, we avoid sending multiple (useless) WriteOk msgs
					print("Quorum reached for " + msg.updateId.toString());
					// TODO CRASH
					if (this.id == 0)
						crash();
					else {
						print("Sending WriteOKs...");
						multicast(new WriteOkRequest(msg.updateId));
					}
				}
			}
		}

		void onWriteOk(WriteOkRequest msg) { // Complete the update process, changing the value of our v + updating the history
			// We need to cancel the corresponding writeOk timeout
			this.writeOkTimeout.get(msg.updateId).cancel();
			this.writeOkTimeout.remove(msg.updateId);

			// Add the update to the completed list and remove it from the pending one
			UpdateIdentifier lastUpdate = msg.updateId;
			Update update = new Update(lastUpdate, pendingUpdates.get(lastUpdate).value);
			completedUpdates.add(update);
			pendingUpdates.remove(msg.updateId);

			print("WriteOk for " + msg.updateId.toString() + ". New value: " + update.v);

			// After handling pendingUpdates of previous epoch through a Synchronization, the future updates' i will takes in account these operation, possibly
			// starting from a not 0 value
			this.i = update.identifier.i;
			this.v = update.v;
		}

		void onPreHeartbeatMessage(PreHeartbeatMessage msg) { // Handle the PreHeartbeatMessage
			print("Sending Heartbeat");
			multicast(new HeartbeatMessage());
		}

		void onHeartbeatMessage(HeartbeatMessage msg) { // Handle the HeartbeatMessage
			// Cohort reset their timers
			if (!this.coordinator.equals(getSelf())) {
				if (this.heartBeatTimer != null)
					this.heartBeatTimer.cancel();
				print("Received Heartbeat. Coordinator alive");

				// Set a new heartbeat timeout
				setHeartbeatTimeout(HEARTHBEAT_TIMEOUT);
			}
		}

		void onHeartbeatTimeout(HeartbeatTimeout msg) { // Handle expired Heartbeat Timeouts
			print("Missing Heartbeat, Coordinator crashed. Sending Election Init...");

			// If we are not in election mode, enter it and start a new election phase
			if (!election) {
				election = true;
				election();

				// Try to contact the first active node of the ring and set a timout
				delay(rnd.nextInt(DELAY_BOUND));
				getFirst().tell(new ElectionInit(), getSelf());
				setElectionInitTimeout(INIT_TIMEOUT, getFirst());
			}
		}

		void onElectionRequest(ElectionRequest msg) { // Handle the ElectionRequest
			// Cancel the timeouts regarding the election starting phase
			if (this.electionStartTimeout != null)
				this.electionStartTimeout.cancel();
			if (this.initTimeout != null)
				this.initTimeout.cancel();

			// Set initStarted to true to prevent multiple elections at the same time
			this.initStarted = true;

			print("Election request recived from " + getSender());
			// If we are not in election mode, enter it and start a new election phase
			if (!election) {
				election = true;
				election();
			}

			delay(rnd.nextInt(DELAY_BOUND)); // Emulate a network delay
			getSender().tell(new ElectionResponse(), getSelf());

			// Add my latest update identifier to the ElectionRequest message
			el_msg = msg;
			if (el_msg.lastUpdateList.get(this.id) == null) { // If it is the first time I receive the message -> My ID is not in the msg
				setElectionCompletedTimeout(SYNC_TIMEOUT); // Set SYNC timeout, the procedure need to terminate or restart

				el_msg.lastUpdateList.put(this.id, new Pair<>(getSelf(), new UpdateIdentifier(this.e, this.i)));

				delay(rnd.nextInt(DELAY_BOUND));
				getNext().tell(el_msg, getSelf()); // I need to find the next node in the link and send forward the election message
				setElectionTimeout(ELECTION_TIMEOUT, getNext());
			} else {
				List<ActorRef> newParticipants = new ArrayList<>(); // List of participants
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

					// Get the most up-to-date node, breaking ties by lower node's ID
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

				if (!(this.id == lastId)) { // If i'm not the new coordinator just forward the Election message
					delay(rnd.nextInt(DELAY_BOUND));
					getNext().tell(el_msg, getSelf());
					setElectionTimeout(ELECTION_TIMEOUT, getNext());
				} else { // If i'm the new coordinator, manage the synchronization procedure
					print("New coordinator found (" + this.id + "). Sending Synchronization");

					this.participants = newParticipants; // Create a new partecipants list
					this.coordinator = getSelf(); // Set myself as the coordinator
					multicast(new Synchronization(this.participants, this.completedUpdates));

					// Handle the pendingUpdates (if needed)
					if (!this.pendingUpdates.isEmpty())
						print("Sending pending updates...");
					for (Map.Entry<UpdateIdentifier, PendingUpdateTuple> entry : this.pendingUpdates.entrySet()) {
						print(entry.getKey() + " - " + entry.getValue());
						Update update = new Update(entry.getKey(), entry.getValue().value);
						multicast(new UpdateRequest(update, getSelf()));
					}
				}
			}
		}

		void onElectionResponse(ElectionResponse msg) { // Handle the ACKs for ElectionRequest
			print("ElectionResponse ACK received from " + getSender());
			this.electionTimeout.cancel(); // Having received the Ack we can cancel the timeout
		}

		void onElectionTimeout(ElectionTimeout msg) { // Handle expired Election Timeout
			this.participants.remove(msg.target); // The node isn't responding, so it must have crashed
			print("Election TIMEOUT. Removed " + msg.target + " from active nodes");

			// If we don't have an election message we have to create a new one
			if (el_msg == null) {
				el_msg = new ElectionRequest();
				el_msg.lastUpdateList.put(this.id, new Pair<>(getSelf(), new UpdateIdentifier(this.e, this.i)));
			}
			delay(rnd.nextInt(DELAY_BOUND));
			getNext().tell(el_msg, getSelf());
			setElectionTimeout(ELECTION_TIMEOUT, getNext()); // Set a new Election Timeout

		}

		void onElectionInitTimeout(ElectioIninitTimeout msg) { // Handle expired Election Init Timeout
			this.participants.remove(msg.target); // The node isn't responding, so it must have crashed
			print("Election Init TIMEOUT. Removed " + msg.target + " from active nodes");

			// If we don't have an election message we have to create a new one
			delay(rnd.nextInt(DELAY_BOUND));
			getFirst().tell(new ElectionInit(), getSelf());
			setElectionInitTimeout(INIT_TIMEOUT, getFirst()); // Set a new Election Init Timeout
		}

		void onElectionStartTimeout(ElectionStartTimeout msg) { // Handle expired Election Start Timeout
			// The election is not started, the node that started the election (by sending the ElectionInit), crashed before starting the election
			// We send another Election Init
			delay(rnd.nextInt(DELAY_BOUND));
			getFirst().tell(new ElectionInit(), getSelf());
			setElectionInitTimeout(INIT_TIMEOUT, getFirst()); // Set a new Election Init Timeout
		}

		void onElectionCompletedTimeout(ElectionCompletedTimeout msg) { // Handle expired Election Complete Timeout
			// The election didn't complete, probably the new coordinator crashed
			// We send another Election Init
			this.initStarted = false; // Reset the initStarted variable

			delay(rnd.nextInt(DELAY_BOUND));
			getFirst().tell(new ElectionInit(), getSelf());
			setElectionInitTimeout(INIT_TIMEOUT, getFirst()); // Set a new Election Init Timeout
		}

		void onSynchronization(Synchronization msg) { // Handle the Synchronization
			// Reset eventual timeouts
			if (this.electionCompletedTimeout != null)
				this.electionCompletedTimeout.cancel();

			print("Sync recived from the new coordinator (" + this.coordinator + ")");
			this.coordinator = getSender(); // Update the coordinator
			this.participants = msg.participants; // Update partecipants
			this.completedUpdates = msg.completedUpdates; // Synchronize to last update

			this.initStarted = false; // Reset the initStarted variable

			// If there are completed updates, copy the v and e values
			if (!msg.completedUpdates.isEmpty()) {
				Update update = msg.completedUpdates.get(msg.completedUpdates.size() - 1);
				this.v = update.v;
				this.e = update.identifier.e;
			}

			this.e++; // Since the epoch has changed, update e
			this.i = 0;

			// Exit election mode
			election = false;
			getContext().become(createReceive());

			if (!getSelf().equals(this.coordinator)) { // If i'm not the coordinator empty the pending updates (we will get them from the coordinator)
				this.pendingUpdates.clear();
			} else { // Otherwise, reschedule the heartbeat msg
				if (this.heartBeatTimer != null)
					this.heartBeatTimer.cancel();
				setHeartbeat(HEARTHBEAT_FREQUENCY);
			}
			print("Current state: (" + this.e + ", " + this.i + ") = " + this.v);
		}

		void onWriteOkTimeout(Timeout msg) { // Handle expired WriteOk Timeout
			print("Missing WriteOk, Coordinator crashed. Sending Election Init...");

			// If we are not in election mode, enter it and start a new election phase
			if (!election) {
				election = true;
				election();

				// Try to contact the first active node of the ring and set a elction init timout
				delay(rnd.nextInt(DELAY_BOUND));
				getFirst().tell(new ElectionInit(), getSelf());
				setElectionInitTimeout(INIT_TIMEOUT, getFirst());
			}

		}

		void print(String s) {
			print(s, false);
		}

		void print(String s, boolean printToFile) { // Logging function
			String role;
			if (getSelf().equals(coordinator)) {
				role = "Coordinator";
			} else {
				role = "Replica";
			}
			System.out.format("%2d %s: %s\n", id, role, s);

			if (printToFile)
				printOnFile(s);
		}

		void printOnFile(String s) { // On-File Logging function
			String role;
			if (getSelf().equals(coordinator)) {
				role = "Coordinator";
			} else {
				role = "Replica";
			}

			String myString = String.format("%2d %s: %s\n", id, role, s);
			String log_path = LOG_DIRECTORY + log_filename;

			try {
				Path path = Paths.get(log_path);
				Files.createDirectories(path.getParent());

				File logFile = new File(log_path);
				if (logFile.createNewFile()) {
					System.out.println("File created: " + logFile.getName());
				}

				FileWriter myWriter = new FileWriter(log_path, true);
				myWriter.write(myString);
				myWriter.close();

			} catch (IOException e) {
				System.out.println("An error occurred during file write.");
				e.printStackTrace();
			}
		}

		void onElectionInit(ElectionInit msg) { // Handle ElectionInit
			// If we are not in election mode, enter it
			if (!this.election) {
				this.election = true;
				election();
			}

			// Sending the ElectionInit ACK
			print("Sending Election Init ACK...");
			delay(rnd.nextInt(DELAY_BOUND));
			getSender().tell(new ElectionInitAck(), getSelf());

			// TODO CRASH
			// if (this.id == 1) {
			// crash();
			// } else {

			// If the election hasn't started, start a new one
			if (this.initStarted == false) {
				print("Starting a new Election");
				this.initStarted = true;

				el_msg = new ElectionRequest();
				el_msg.lastUpdateList.put(this.id, new Pair<>(getSelf(), new UpdateIdentifier(this.e, this.i)));
				delay(rnd.nextInt(DELAY_BOUND));
				getNext().tell(el_msg, getSelf());
				setElectionTimeout(ELECTION_TIMEOUT, getNext());
				if (this.id == 1)
					crash();
			} else {
				print("Election already in progress. Sending ACK without starting a new one");
			}
			// }

		}

		void onElectionInitAck(ElectionInitAck msg) { // Handle the ACKs for ElectionInit
			this.initTimeout.cancel(); // Cancel the ElectionInit timeout
			setElectionStartTimeout(START_TIMEOUT); // Set a election start timout
		}

		@Override
		public Receive createReceive() { // Map message to their handler for the normal mode
			return receiveBuilder().match(StartMessage.class, this::onStartMessage).match(IssueWrite.class, this::onWrite)
					.match(IssueRead.class, this::onRead).match(HeartbeatMessage.class, this::onHeartbeatMessage)
					.match(ElectionRequest.class, this::onElectionRequest).match(UpdateRequest.class, this::onUpdateRequest)
					.match(UpdateResponse.class, this::onUpdateResponse).match(WriteOkRequest.class, this::onWriteOk)
					.match(PreHeartbeatMessage.class, this::onPreHeartbeatMessage).match(ElectionInit.class, this::onElectionInit)
					.match(ElectionInitAck.class, this::onElectionInitAck).match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
					.match(Timeout.class, this::onWriteOkTimeout).match(ElectioIninitTimeout.class, this::onElectionInitTimeout)
					.build();
		}

		public Receive crashed() { // Map message to their handler for the crashed mode
			return receiveBuilder().matchAny(msg -> {
			}).build();
		}

		public Receive electionMode() { // Map message to their handler for the election mode
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

	/*-- Main ------------------------------------------------------------------*/
	public static void main(String[] args) throws InterruptedException {

		// Create the name for the log file
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH-mm-ss");
		String formattedDateTime = LocalDateTime.now().format(formatter);
		log_filename = formattedDateTime + ".log";

		// Create the actor system
		final ActorSystem system = ActorSystem.create("helloakka");

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

		// Send the start messages to the coordinator
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
