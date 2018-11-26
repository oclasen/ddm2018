package de.hpi.octopus.actors;

import java.io.Serializable;
import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.*;

import akka.actor.*;
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
    public static class PrefixCompletionMessage extends CompletionMessage {
        private static final long serialVersionUID = -3124246288264562748L;
        private ArrayList<ArrayList<Integer>> combinations;
    }

    @AllArgsConstructor @Data
    public static class GeneCompletionMessage extends CompletionMessage {
        private static final long serialVersionUID = -3128246988264562748L;
        private int originId;
        private int partnerId;
        private int length;
    }

    @AllArgsConstructor @Data
    public static class HashCompletionMessage extends CompletionMessage {
        private static final long serialVersionUID = -3128292988264562748L;
        private ArrayList<ArrayList<String>> hashes;
    }

    /////////////////
    // Actor State //
    /////////////////

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private final Queue<WorkMessage> unassignedWork = new LinkedList<>();
    private final Queue<ActorRef> idleWorkers = new LinkedList<>();
    private final Map<ActorRef, WorkMessage> busyWorkers = new HashMap<>();

    private final ArrayList<List<Integer>> tempPartners = new ArrayList<List<Integer>>();

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
                .match(PrefixCompletionMessage.class, this::handle)
                .match(GeneCompletionMessage.class, this::handle)
                .match(HashCompletionMessage.class, this::handle)
                .matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(RegistrationMessage message) {
        this.context().watch(this.sender());

        this.log.info("Registered {}", this.sender());
        this.assign(this.sender());
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
        this.assign();
    }

    private void handle(PasswordCompletionMessage message) {
        busyWorkers.remove(this.sender());
        idleWorkers.add(this.sender());
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
            for (int i = 0; i < students.size(); i++){
                this.log.info(students.get(i).toString());
            }
            this.log.info("password cracking completed");
            createPrefixWork();
        }
        this.assign(this.sender());
    }

    private void handle(PrefixCompletionMessage message){
        busyWorkers.remove(this.sender());
        idleWorkers.add(this.sender());
        this.log.info("prefix complete");
        ArrayList<ArrayList<Integer>> combinations = message.combinations;

        for (int i = 0; i < combinations.size(); i++){
            for (int j = 0; j < combinations.get(i).size();j++){
                students.get(j).add(String.valueOf(combinations.get(i).get(j)));
            }
        }

        if (unassignedWork.isEmpty() && busyWorkers.isEmpty()){
            for (int i = 0; i < students.size(); i++){
                this.log.info(students.get(i).toString());
            }
            createGeneWork();
        }

    }

    private void handle(GeneCompletionMessage message) {
        busyWorkers.remove(this.sender());
        idleWorkers.add(this.sender());
        this.log.info("geneComplete");
        int originId = message.originId;
        int partnerId = message.partnerId;
        int length = message.length;
        List<Integer> partner = new ArrayList<Integer>();
        partner.add(partnerId);
        partner.add(length);
        if (tempPartners.get(originId-1).get(1) < length){
            tempPartners.set(originId-1, partner);
        }
        if (unassignedWork.isEmpty() && busyWorkers.isEmpty()){
            for (int i = 0; i < students.size(); i++){
                students.get(i).set(4, Long.toString(tempPartners.get(i).get(0)));
            }
            for (int i = 0; i < students.size(); i++){
                this.log.info(students.get(i).toString());
            }
            createHashWork();
        }

        this.assign(this.sender());
    }

    private void handle(HashCompletionMessage message) {
        busyWorkers.remove(this.sender());
        idleWorkers.add(this.sender());
        this.log.info("hashComplete");
        ArrayList<ArrayList<String>> hashes = message.hashes;
        for (int i = 0; i < hashes.size(); i++) {
            int id = Integer.parseInt(hashes.get(i).get(0));
            String hash = hashes.get(i).get(1);

            students.get(id - 1).set(5, hash);
        }

        if (unassignedWork.isEmpty() && busyWorkers.isEmpty()){
            for (int i = 0; i < students.size(); i++){
                this.log.info(students.get(i).toString());
            }
            for (int i = 0; i < idleWorkers.size(); i++) {
                ActorRef worker = idleWorkers.poll();
                worker.tell(PoisonPill.getInstance(), this.self());
            }

        }

        this.assign(this.sender());
    }

    private void createPrefixWork(){
        this.log.info("creating prefixWork");
        List<String> passwords = new ArrayList<String>();
        for (int j = 0; j < students.size(); j++){
            passwords.add(students.get(j).get(2));
        }
        this.log.info("list of passwords created");
        double steps = Math.pow(2, students.size())-1;
        for (double i = 0; i < steps; i+=Math.floor(steps/20)){
            BigInteger signsBegin = BigDecimal.valueOf(i).toBigInteger();
            long range = (long) Math.floor(steps/20);
            unassignedWork.add(new Worker.LinearCombinationWorkMessage(passwords, signsBegin, range));
        }
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
                int id = i+1;
                unassignedWork.add(new Worker.GeneWorkMessage(id, gene, studentPackage));
            }
        }
    }

    private void createHashWork() {
        for(int j = 0; j <students.size(); j+=4) {
            List<ArrayList<String>> studentPackage = students.subList(j, Math.min(j + 4, students.size() - 1));
            ArrayList<ArrayList<Integer>> prefixPartners = new ArrayList<ArrayList<Integer>>();
            for (int i = 0; i < studentPackage.size();i++) {
                ArrayList<Integer> prefixPartner = new ArrayList<Integer>();
                int id = Integer.parseInt(studentPackage.get(i).get(0));
                int prefix = Integer.parseInt(studentPackage.get(i).get(3));
                int partner = Integer.parseInt(studentPackage.get(i).get(4));

                prefixPartner.add(id);
                prefixPartner.add(prefix);
                prefixPartner.add(partner);
                prefixPartners.add(prefixPartner);
            }
            unassignedWork.add(new Worker.HashMiningWorkMessage(prefixPartners));
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
            this.log.info("no worker");
            return;
        }
        this.log.info(worker.toString());
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