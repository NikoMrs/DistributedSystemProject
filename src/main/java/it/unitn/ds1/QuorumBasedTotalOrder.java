package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.lang.Thread;
import java.util.Collections;

import java.io.IOException;

public class QuorumBasedTotalOrder {

	// General Configuration
  final static int N_PARTICIPANTS = 3;
  final static int QUORUM = (N_PARTICIPANTS/2) + 1;

	public static class Update{
		public int e;
		public int i;
		public int v;

		public Update(int e, int i, int v){
			this.e = e;
			this.i = i;
			this.v = v;
		}
	}

	public enum Operation {READ, WRITE}

	// Timeouts Configuration
  final static int WRITEOK_TIMEOUT = 3000;      					// timeout started after receiving the update msg. Detects a coordinator crash
  final static int BROADCAST_INITIATE_TIMEOUT = 1000;  		// timeout started after sending a write request. Detects a coordinator crash
	final static int HEARTHBEAT_TIMEOUT = 10000;  					// timeout started after receiving a heartbeat msg. Detects a coordinator crash
	final static int REPLICA_TIMEOUT = 2000;								// timeout started after sending a msg to a replica. Detects a replica crash

  // Messages Configuration
  public static class StartMessage implements Serializable {		// Start message that sends the list of participants to everyone
    public final List<ActorRef> group;
    public StartMessage(List<ActorRef> group) {
      this.group = Collections.unmodifiableList(new ArrayList<>(group));
    }
  }

	public static class IssueWrite implements Serializable {		// Sent from a client to a replica. Asks for a Write operation
		public int value;
		IssueWrite(int value){ this.value = value; };
	}

	public static class IssueRead implements Serializable {}		// Sent from a client to a replica. Asks for a Read operation

	public static class ForwardUpdate implements Serializable {				// Sent from a replica to the coordinator. Forward an update request
		public final Update update ;
		ForwardUpdate(int e, int i, int value){
			this.update = new Update(e, i, value);
		};
	}

	public static class UpdateRequest implements Serializable {				// Sent from the coordinator to all replicas. Initiate the update protocol
		public final Update update ;
		UpdateRequest(Update update){ this.update = update; };
	}

	public static class UpdateResponse implements Serializable {}			// Sent from a replica to the coordinator. ACK for the UpdateRequest

	public static class WriteOkRequest implements Serializable {}			// Sent from the coordinator to all replicas. Complete the update, modifying the value v

	public static class ElectionRequest implements Serializable {			// Sent from a replica to its successor. Initiate a coordinator election
		// TODO: Aggiungere gli update noti da ogni nodo
	}

	public static class ElectionResponse implements Serializable {}		// Sent from a replica to its predecessor. ACK for the ElectionRequest

  public static class SynchronizationRequest implements Serializable {}		// Sent to the most updated replica to every other replica. Completes the election, updating the old replicas' v

	public static class HeartbeatMessage implements Serializable {}		// Sent periodically from the coordinator to all replicas.

  public static class Timeout implements Serializable {}


   /*-- Common functionality for both Coordinator and Replicas ------------*/

	public abstract static class Node extends AbstractActor {
	  protected int id;                           // node ID
	  protected List<ActorRef> participants;      // list of participant nodes
	  protected int v;         // decision taken by this node
		protected ArrayList<Update> pendingUpdates;
		protected ArrayList<Update> completedUpdates;
		protected int coordinatorId;

	  public Node(int id) {
		  super();
		  this.id = id;
	  }

	  void setGroup(StartMessage sm) {
		  participants = new ArrayList<>();
		  for (ActorRef b: sm.group) {
			  if (!b.equals(getSelf())) {
					// copying all participant refs except for self
				  this.participants.add(b);
			 	}
		 	}
		 	print("starting with " + sm.group.size() + " peer(s)");
	 	}

	  // emulate a crash and a recovery in a given time
	  void crash(int recoverIn) {
		  getContext().become(crashed());
		  print("CRASH!!!");
	  }

	  // emulate a delay of d milliseconds
	  void delay(int d) {
		  try {Thread.sleep(d);} catch (Exception ignored) {}
	  }

	  void multicast(Serializable m) {
	 	 for (ActorRef p: participants)
	 		 p.tell(m, getSelf());
	  }

	  // a multicast implementation that crashes after sending the first message
	  void multicastAndCrash(Serializable m, int recoverIn) {
		  for (ActorRef p: participants) {
		 	  p.tell(m, getSelf());
		 	  crash(recoverIn); return;
		  }
	  }

	  // schedule a Timeout message in specified time
	  void setTimeout(int time) {
			getContext().system().scheduler().scheduleOnce(
					Duration.create(time, TimeUnit.MILLISECONDS),
					getSelf(),
					new Timeout(), // the message to send
					getContext().system().dispatcher(), getSelf()
			);
		}

		void handleWrite(int value){
			if(coordinatorId == this.id){
				// TODO: Procedere con l'update protocol:
				// Opzione 1: Aggiorno pendingUpdates e invio un UpdateRequest alle replicas
				// Opzione 2: Invio a tutti (me compreso) l'UpdateRequest
			}
			else{
				// TODO: Invio un issue al coordinator
			}
		}

		void handleRead(){
			getSender().tell(v, getSelf());
		}

		// First step of the update protocol, adding a step to the history + sends ACK
		void updateValue(Update update) {
			pendingUpdates.add(update);
			getSender().tell(new UpdateResponse(), getSelf());
		}

	 	// Complete the update process, changing the value of our v + updating the history
		void completeWrite() {

	  }

	 // a simple logging function
	 void print(String s) {
		 System.out.format("%2d: %s\n", id, s);
	 }

	 @Override
	 public Receive createReceive() {

		 // Empty mapping: we'll define it in the inherited classes
		 return receiveBuilder().build();
	 }

	 public Receive crashed() {
		 return null;
//				 receiveBuilder()
//						 .match(Recovery.class, this::onRecovery)
//						 .matchAny(msg -> {})
//						 .build();
	 }

	}

  // /*-- Coordinator -----------------------------------------------------------*/

  // public static class Coordinator extends Node {

  //   // here all the nodes that sent YES are collected
  //   private final Set<ActorRef> yesVoters = new HashSet<>();

  //   boolean allVotedYes() { // returns true if all voted YES
  //     return yesVoters.size() >= N_PARTICIPANTS;
  //   }

  //   public Coordinator() {
  //     super(-1); // the coordinator has the id -1
  //   }

  //   static public Props props() {
  //     return Props.create(Coordinator.class, Coordinator::new);
  //   }

  //   @Override
  //   public Receive createReceive() {
  //     return receiveBuilder()
  //       .match(Recovery.class, this::onRecovery)
  //       .match(StartMessage.class, this::onStartMessage)
  //       .match(VoteResponse.class, this::onVoteResponse)
  //       .match(Timeout.class, this::onTimeout)
  //       .match(DecisionRequest.class, this::onDecisionRequest)
  //       .build();
  //   }

  //   public void onStartMessage(StartMessage msg) {                   /* Start */
  //     setGroup(msg);
  //     print("Sending vote request");
  //     multicast(new VoteRequest());
  //     //multicastAndCrash(new VoteRequest(), 3000);
  //     setTimeout(VOTE_TIMEOUT);
  //     //crash(5000);
  //   }

  //   public void onVoteResponse(VoteResponse msg) {                    /* Vote */
  //     if (hasDecided()) {

  //       // we have already decided and sent the decision to the group, 
  //       // so do not care about other votes
  //       return;
  //     }
  //     Vote v = (msg).vote;
  //     if (v == Vote.YES) {
  //       yesVoters.add(getSender());
  //       if (allVotedYes()) {
  //         fixDecision(Decision.COMMIT);
  //         //if (id==-1) {crash(3000); return;}
  //         //multicast(new DecisionResponse(decision));
  //         multicastAndCrash(new DecisionResponse(decision), 3000);
  //       }
  //     } 
  //     else { // a NO vote

  //       // on a single NO we decide ABORT
  //       fixDecision(Decision.ABORT);
  //       multicast(new DecisionResponse(decision));
  //     }
  //   }

  //   public void onTimeout(Timeout msg) {
  //     if (!hasDecided()) {
  //       print("Timeout");

  //       // TODO 1: coordinator timeout action
  //     }
  //   }

  //   @Override
  //   public void onRecovery(Recovery msg) {
  //     getContext().become(createReceive());

  //     // TODO 2: coordinator recovery action
  //   }
  // }

  // /*-- Participant -----------------------------------------------------------*/
  // public static class Participant extends Node {
  //   ActorRef coordinator;
  //   public Participant(int id) { super(id); }

  //   static public Props props(int id) {
  //     return Props.create(Participant.class, () -> new Participant(id));
  //   }

  //   @Override
  //   public Receive createReceive() {
  //     return receiveBuilder()
  //       .match(StartMessage.class, this::onStartMessage)
  //       .match(VoteRequest.class, this::onVoteRequest)
  //       .match(DecisionRequest.class, this::onDecisionRequest)
  //       .match(DecisionResponse.class, this::onDecisionResponse)
  //       .match(Timeout.class, this::onTimeout)
  //       .match(Recovery.class, this::onRecovery)
  //       .build();
  //   }

  //   public void onStartMessage(StartMessage msg) {
  //     setGroup(msg);
  //   }

  //   public void onVoteRequest(VoteRequest msg) {
  //     this.coordinator = getSender();
  //     //if (id==2) {crash(5000); return;}    // simulate a crash
  //     //if (id==2) delay(4000);              // simulate a delay
  //     if (predefinedVotes[this.id] == Vote.NO) {
  //       fixDecision(Decision.ABORT);
  //     }
  //     print("sending vote " + predefinedVotes[this.id]);
  //     this.coordinator.tell(new VoteResponse(predefinedVotes[this.id]), getSelf());
  //     setTimeout(DECISION_TIMEOUT);
  //   }

  //   public void onTimeout(Timeout msg) {
  //     if (!hasDecided()) {
  //       print("Timeout. Asking around.");

  //       // TODO 3: participant termination protocol
  //     }
  //   }

  //   @Override
  //   public void onRecovery(Recovery msg) {
  //     getContext().become(createReceive());

  //     // We don't handle explicitly the "not voted" case here
  //     // (in any case, it does not break the protocol)
  //     if (!hasDecided()) {
  //       print("Recovery. Asking the coordinator.");
  //       coordinator.tell(new DecisionRequest(), getSelf());
  //       setTimeout(DECISION_TIMEOUT);
  //     }
  //   }

  //   public void onDecisionResponse(DecisionResponse msg) { /* Decision Response */

  //     // store the decision
  //     fixDecision(msg.decision);
  //   }
  // }

  // /*-- Main ------------------------------------------------------------------*/
  // public static void main(String[] args) {

  //   // Create the actor system
  //   final ActorSystem system = ActorSystem.create("helloakka");

  //   // Create the coordinator
  //   ActorRef coordinator = system.actorOf(Coordinator.props(), "coordinator");

  //   // Create participants
  //   List<ActorRef> group = new ArrayList<>();
  //   for (int i=0; i<N_PARTICIPANTS; i++) {
  //     group.add(system.actorOf(Participant.props(i), "participant" + i));
  //   }

  //   // Send start messages to the participants to inform them of the group
  //   StartMessage start = new StartMessage(group);
  //   for (ActorRef peer: group) {
  //     peer.tell(start, null);
  //   }

  //   // Send the start messages to the coordinator
  //   coordinator.tell(start, null);

  //   try {
  //     System.out.println(">>> Press ENTER to exit <<<");
  //     System.in.read();
  //   } 
  //   catch (IOException ignored) {}
  //   system.terminate();
  // }
}
