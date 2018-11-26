package de.hpi.octopus.actors;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.*;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.octopus.OctopusMaster;
import de.hpi.octopus.actors.Profiler.PasswordCompletionMessage;
import de.hpi.octopus.actors.Profiler.RegistrationMessage;
import de.hpi.octopus.utils.GeneComparison;
import de.hpi.octopus.utils.LinearCombination;
import de.hpi.octopus.utils.Passwordcracker;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Worker extends AbstractActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @SuppressWarnings("unused")
	public static class WorkMessage implements Serializable {
		private static final long serialVersionUID = -7643194361868862395L;
		private WorkMessage() {}
	}

	@Data @AllArgsConstructor
	public static class PasswordWorkMessage extends WorkMessage {
		private static final long serialVersionUID = -3129246284264562748L;

		private ArrayList<List<String>> victims;

	}

	@Data @AllArgsConstructor
	public static class LinearCombinationWorkMessage extends WorkMessage {
		private static final long serialVersionUID = -2492671797894687456L;

		private List<String> passwords;
		private BigInteger signsBegin;
		private long range;

	}

	@Data @AllArgsConstructor
	public static class GeneWorkMessage extends WorkMessage {
		private static final long serialVersionUID = -1146589161762748486L;

		private long originalId;
		private String originalGene;
		private List<ArrayList<String>> potentialPartners;

	}

	@Data @AllArgsConstructor
	public static class HashMiningWorkMessage extends WorkMessage {
		private static final long serialVersionUID = -3498714556892986224L;

		private ArrayList<String> prefixAndIds;

	}

	/////////////////
	// Actor State //
	/////////////////
	
	private final LoggingAdapter log = Logging.getLogger(this.context().system(), this);
	private final Cluster cluster = Cluster.get(this.context().system());

	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	@Override
	public void preStart() {
		this.cluster.subscribe(this.self(), MemberUp.class);
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
				.match(PasswordWorkMessage.class, this::handle)
				.match(GeneWorkMessage.class, this:: handle)
				.match(LinearCombinationWorkMessage.class, this::handle)
				.match(HashMiningWorkMessage.class, this::handle)
				.matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
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
		if (member.hasRole(OctopusMaster.MASTER_ROLE))
			this.getContext()
				.actorSelection(member.address() + "/user/" + Profiler.DEFAULT_NAME)
				.tell(new RegistrationMessage(), this.self());
	}
	
	private void handle(PasswordWorkMessage message) {
		ArrayList<List<String>> crackedVictims = new ArrayList<>();
		for(List<String> victim : message.victims) {
			String crackedPassword = Passwordcracker.crack(victim.get(1));
			List<String> crackedVictim = new ArrayList<>();
			crackedVictim.add(victim.get(0));
			crackedVictim.add(crackedPassword);
			crackedVictims.add(crackedVictim);
		}
		this.sender().tell(new PasswordCompletionMessage(crackedVictims), this.self());
	}

	private void handle(GeneWorkMessage message) {
		int partnerId = -1;
		int length = 0;
		for(ArrayList<String> entry : message.potentialPartners) {
			int potlength = GeneComparison.findLongestSubstring(message.originalGene, entry.get(1));
			if (potlength > length) {
				length = potlength;
				partnerId = Integer.valueOf(entry.get(0));
			}
		}
		this.sender().tell(new Profiler.GeneCompletionMessage(message.originalId, partnerId, length), this.self());
	}

	private void handle(LinearCombinationWorkMessage message) {

	}

	private void handle(HashMiningWorkMessage message) {

	}

}