package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.octopus.actors.Worker.WorkMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Profiler extends AbstractActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "profiler";

    public static Props props() {
        return Props.create(Profiler.class);
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data @AllArgsConstructor
    public static class RegistrationMessage implements Serializable {
        private static final long serialVersionUID = 4545299661052078209L;
    }

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class TaskMessage implements Serializable {
        private static final long serialVersionUID = -8330958742629706627L;
        private TaskMessage() {}
        private ArrayList<ArrayList<String>> students;
    }

    @Data  @SuppressWarnings("unused")
    public static class CompletionMessage implements Serializable {
        private static final long serialVersionUID = -6823011111281387872L;
        public enum status {MINIMAL, EXTENDABLE, FALSE, FAILED}
        private CompletionMessage() {}
        private status result;
    }

    @AllArgsConstructor @Data
    public static class PasswordCompletionMessage extends CompletionMessage {
        private static final long serialVersionUID = -3129246288264562748L;

        private ArrayList<List<String>> victims;


    }

    /////////////////
    // Actor State //
    /////////////////

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private final Queue<WorkMessage> unassignedWork = new LinkedList<>();
    private final Queue<ActorRef> idleWorkers = new LinkedList<>();
    private final Map<ActorRef, WorkMessage> busyWorkers = new HashMap<>();

    private ArrayList<ArrayList<String>> students = new ArrayList<ArrayList<String>>();


    private TaskMessage task;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(RegistrationMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .match(TaskMessage.class, this::handle)
                .match(PasswordCompletionMessage.class, this::handle)
                .matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(RegistrationMessage message) {
        this.context().watch(this.sender());

        this.assign(this.sender());
        this.log.info("Registered {}", this.sender());
    }

    private void handle(Terminated message) {
        this.context().unwatch(message.getActor());

        if (!this.idleWorkers.remove(message.getActor())) {
            WorkMessage work = this.busyWorkers.remove(message.getActor());
            if (work != null) {
                this.assign();
            }
        }
        this.log.info("Unregistered {}", message.getActor());
    }

    private void handle(TaskMessage message) {
        if (this.task != null)
            this.log.error("The profiler actor can process only one task in its current implementation!");
        this.log.info("task received");


        students = message.students;

        for(int i = 0; i<students.size(); i+=4) {
            List<ArrayList<String>> studentPackage = students.subList(i,Math.min(i+4,students.size()-1));
            ArrayList<List<String>> workPackage = new ArrayList<List<String>>();
            for (int j = 0; j<studentPackage.size();j++){
                ArrayList<String> idPw = new ArrayList<String>();
                idPw.add(studentPackage.get(j).get(0));
                idPw.add(studentPackage.get(j).get(2));

                workPackage.add(idPw);
            }
            unassignedWork.add(new Worker.PasswordWorkMessage(workPackage));
        }

        this.log.info("work generated");


        this.task = message;
        while(!unassignedWork.isEmpty()) {
            this.assign();
        }
    }

//    private void handle(CompletionMessage message) {
//        ActorRef worker = this.sender();
//        WorkMessage work = this.busyWorkers.remove(worker);
//
//        this.log.info("Completed: [{},{}]", Arrays.toString(work.getX()), Arrays.toString(work.getY()));
//
//        switch (message.getResult()) {
//            case MINIMAL:
//                this.report(work);
//                break;
//            case EXTENDABLE:
//                this.split(work);
//                break;
//            case FALSE:
//                // Ignore
//                break;
//            case FAILED:
//                this.assign(work);
//                break;
//        }
//
//        this.assign(worker);
//    }

    private void handle(PasswordCompletionMessage message) {
        this.log.info("keeeeeekse");
        this.log.info(message.victims.toString());
    }

    private void assign() {
        WorkMessage work = this.unassignedWork.poll();
        ActorRef worker = this.idleWorkers.poll();

        this.log.info(work.toString());

        if (worker == null) {
            this.unassignedWork.add(work);
            return;
        }

        this.busyWorkers.put(worker, work);
        worker.tell(work, this.self());
    }

    private void assign(ActorRef worker) {
        WorkMessage work = this.unassignedWork.poll();

        if (work == null) {
            this.idleWorkers.add(worker);
            return;
        }

        this.busyWorkers.put(worker, work);
        worker.tell(work, this.self());
    }

    private void report(WorkMessage work) {

    }

    private void split(WorkMessage work) {

    }
}