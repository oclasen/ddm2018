package de.hpi.ddm1HenschelClasen.actors;

import akka.actor.*;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * The reaper resides in any actor system and terminates it as soon as all watched actors have terminated.
 */
public class Reaper extends AbstractLoggingActor {

	public static final String DEFAULT_NAME = "reaper";

	/**
	 * Create the {@link Props} necessary to instantiate new {@link Reaper} actors.
	 *
	 * @return the {@link Props}
	 */
	public static Props props() {
		return Props.create(Reaper.class);
	}

	/**
	 * Asks the {@link Reaper} to watch for the termination of the sender.
	 */
	public static class WatchMeMessage implements Serializable {

		private static final long serialVersionUID = -5201749681392553264L;
	}
	public static class WatchWorkerMessage implements Serializable {

		private static final long serialVersionUID = -5201749681392553264L;
	}

	/**
	 * Find the reaper actor of this actor system and let it watch the given actor.
	 *
	 * @param actor the actor to be watched
	 * @see #DEFAULT_NAME the name of the default reaper
	 */
	public static void watchWithDefaultReaper(AbstractActor actor) {
		ActorSelection defaultReaper = actor.getContext().getSystem().actorSelection("/user/" + DEFAULT_NAME);
		defaultReaper.tell(new WatchMeMessage(), actor.getSelf());
	}

	public static void watchWorker(AbstractActor actor) {
		ActorSelection defaultReaper = actor.getContext().getSystem().actorSelection("/user/" + DEFAULT_NAME);
		defaultReaper.tell(new WatchWorkerMessage(), actor.getSelf());
	}

	// A reference to all actors whose life is watched by this reaper
	private final Set<ActorRef> watchees = new HashSet<>();
	private final Set<ActorRef> workers = new HashSet<>();

	@Override
	public void preStart() throws Exception {
		super.preStart();
		
		// Log the start event
		log().info("Started {}...", this.getSelf());
	}
	
	@Override
	public void postStop() throws Exception {
		super.postStop();
		
		// Log the stop event
		this.log().info("Stopped {}.", this.getSelf());
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(WatchMeMessage.class, this::handle)
				.match(WatchWorkerMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.matchAny(object -> this.log().error(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}

	private void handle(WatchMeMessage message) {
		
		// Find the sender of this message
		final ActorRef sender = this.getSender();
		
		// Watch the sender if it is not already on the watch list
		if (this.watchees.add(sender)) {
			this.getContext().watch(sender);
			this.log().info("Started watching {}.", sender);
		}
	}

	private void handle(WatchWorkerMessage message) {

		// Find the sender of this message
		final ActorRef sender = this.getSender();

		// Watch the sender if it is not already on the watch list
		if (this.workers.add(sender)) {
			this.getContext().watch(sender);
			this.log().info("Started watching worker {}.", sender);
		}
	}

	private void handle(Terminated message) {
		
		// Find the sender of this message
		final ActorRef sender = this.getSender();
		
		// Remove the sender from the watch list reaping its soul and terminate the entire actor system if this was its last actor
		if (this.watchees.remove(sender)) {
			this.log().info("Reaping {}.", sender);
			if (this.workers.remove(sender)) {
				if (this.workers.isEmpty()) {
					killAll();
				}
			}
			if (this.watchees.isEmpty()) {
				this.log().info("Every local actor has been reaped. Terminating the actor system...");
				CoordinatedShutdown.get(this.getContext().getSystem()).runAll(CoordinatedShutdown.unknownReason());
			}
		} else {
			this.log().error("Got termination message from unwatched {}.", sender);
		}
	}

	private void killAll() {
		for (ActorRef a : watchees) {
			a.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
	}
}
