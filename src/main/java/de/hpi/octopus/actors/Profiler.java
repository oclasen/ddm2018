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

    @AllArgsConstructor @Data
    public static class GeneCompletionMessage extends CompletionMessage {
        private static final long serialVersionUID = -3128246988264562748L;
        private long id;
        private ArrayList<List<Long>> potentialPatners;
    }

    @AllArgsConstructor @Data
    public static class HashCompletionMessage extends CompletionMessage {
        private static final long serialVersionUID = -3128292988264562748L;
        private int id;
        private String hash;
    }

    /////////////////
    // Actor State //
    /////////////////

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private final Queue<WorkMessage> unassignedWork = new LinkedList<>();
    private final Queue<ActorRef> idleWorkers = new LinkedList<>();
    private final Map<ActorRef, WorkMessage> busyWorkers = new HashMap<>();

    private final ArrayList<List<String>> results = new ArrayList<List<String>>();

    private final ArrayList<List<Long>> tempPartners = new ArrayList<List<Long>>();

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
        this.assign();
    }

    private void handle(PasswordCompletionMessage message) {
        this.log.info("keeeeeekse");
        this.log.info(message.victims.toString());
        ArrayList<List<String>> victims = message.victims;
        for (int i = 0; i < victims.size(); i++){
            List<String> singleResult = victims.get(i);
            int id = Integer.parseInt(singleResult.get(0));
            String pw = singleResult.get(1);
            ArrayList<String> student = students.get(id);
            student.set(2, pw);
            students.set(id-1, student);
        }
        if (unassignedWork.isEmpty() && busyWorkers.isEmpty()){
            //createPrefixWork();
        }
        this.assign(this.sender());
    }

    private void handle(GeneCompletionMessage message) {
        this.log.info("geneComplete");
        ArrayList<List<Long>> potentialPartners = message.potentialPatners;
        for (int i = 0; i < potentialPartners.size(); i++){
            List<Long> potentialPartner = potentialPartners.get(i);
            int id = Math.toIntExact(message.id);
            if (tempPartners.get(id-1).get(1) < potentialPartner.get(1)){
                tempPartners.set(id-1,potentialPartner);
            }
        }
        if (unassignedWork.isEmpty() && busyWorkers.isEmpty()){
            for (int i = 0; i < students.size(); i++){
                students.get(i).set(4, Long.toString(tempPartners.get(i).get(0)));
            }
            //createHashWork();
        }

        this.assign(this.sender());
    }

    private void handle(HashCompletionMessage message) {
        this.log.info("hashComplete");
        int id = message.id;
        String hash = message.hash;

        students.get(id-1).set(5, hash);

        if (unassignedWork.isEmpty() && busyWorkers.isEmpty()){
            for (int i = 0; i < students.size(); i++){
                this.log.info(students.get(i).toString());
            }

        }

        this.assign(this.sender());
    }

    private void createGeneWork() {
        for (int i = 0; i < students.size(); i++){
            for(int j = 0; j <students.size(); j+=4) {
                List<ArrayList<String>> studentPackage = students.subList(j, Math.min(j + 4, students.size() - 1));
                for (int k = 0; k < studentPackage.size(); k++){
                    studentPackage.get(k).remove(4);
                    studentPackage.get(k).remove(2);
                    studentPackage.get(k).remove(1);
                }
                String gene = students.get(i).get(3);
                students.get(i).remove(3);
                long id = i+1;
                unassignedWork.add(new Worker.GeneWorkMessage(id, gene, studentPackage));
            }
        }
    }

    private void createHashWork() {
        for (int i = 0; i < students.size(); i++){
            String id = students.get(i).get(0);
            String prefix = students.get(i).get(3);
            String partner = students.get(i).get(4);
            ArrayList<String> prefixPartner = new ArrayList<String>();
            prefixPartner.add(id);
            prefixPartner.add(prefix);
            prefixPartner.add(partner);
            unassignedWork.add(new Worker.HashMiningWorkMessage(prefixPartner));
        }
    }

    private void assign() {
        WorkMessage work = this.unassignedWork.poll();
        ActorRef worker = this.idleWorkers.poll();
        if (work == null){
            return;
        }
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
}