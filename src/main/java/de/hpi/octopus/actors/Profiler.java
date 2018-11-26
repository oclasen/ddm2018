package de.hpi.octopus.actors;

import java.io.Serializable;
import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.*;

import akka.actor.*;
import akka.cluster.Cluster;
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

    @Override
    public void preStart() {
        Reaper.watchWithDefaultReaper(this);
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
        public PasswordCompletionMessage() {
        }
        private ArrayList<List<String>> victims;


    }

    @AllArgsConstructor @Data
    public static class PrefixCompletionMessage extends CompletionMessage {
        private static final long serialVersionUID = -3124246288264562748L;
        public PrefixCompletionMessage(){}
        private ArrayList<ArrayList<Integer>> combinations;


    }

    @AllArgsConstructor @Data
    public static class GeneCompletionMessage extends CompletionMessage {
        private static final long serialVersionUID = -3128246988264562748L;
        public GeneCompletionMessage(){

        }
        private int originId;
        private int partnerId;
        private int length;


    }

    @AllArgsConstructor @Data
    public static class HashCompletionMessage extends CompletionMessage {
        private static final long serialVersionUID = -3128292988264562748L;
        public HashCompletionMessage(){

        }
        private ArrayList<String> hash;


    }

    /////////////////
    // Actor State //
    /////////////////

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private final Queue<WorkMessage> unassignedWork = new LinkedList<>();
    private final Queue<ActorRef> idleWorkers = new LinkedList<>();
    private final Map<ActorRef, WorkMessage> busyWorkers = new HashMap<>();

    private final ArrayList<List<Integer>> tempPartners = new ArrayList<List<Integer>>();
    private final ArrayList<ArrayList<String>> hashValues = new ArrayList<ArrayList<String>>();

    private ArrayList<ArrayList<String>> students = new ArrayList<ArrayList<String>>();

    private Boolean prefixDone = false;

    private long startTime = 0;


    //private final ActorRef master = null;


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
        this.log.info("Cluster starts working.");
        students = message.students;
        startTime = System.currentTimeMillis();


        for (int j = 0; j < students.size(); j++){
            ArrayList<Integer> partner = new ArrayList<>();
            partner.add(0);
            partner.add(0);
            tempPartners.add(partner);
        }

        for(int i = 0; i < students.size(); i+=3) {
            List<ArrayList<String>> studentPackage = students.subList(i,Math.min(i+2,students.size()-1)+1);
            ArrayList<List<String>> workPackage = new ArrayList<List<String>>();
            for (int j = 0; j < studentPackage.size();j++){
                ArrayList<String> idPw = new ArrayList<String>();
                idPw.add(studentPackage.get(j).get(0));
                idPw.add(studentPackage.get(j).get(2));

                workPackage.add(idPw);
            }
            unassignedWork.add(new Worker.PasswordWorkMessage(workPackage));
        }
        this.assignAll();
    }

    private void handle(PasswordCompletionMessage message) {
        this.log.info(message.toString());
        ArrayList<List<String>> victims = message.victims;
        for (int i = 0; i < victims.size(); i++){
            List<String> singleResult = victims.get(i);
            int id = Integer.parseInt(singleResult.get(0));
            String pw = singleResult.get(1);
            ArrayList<String> student = students.get(id-1);
            student.set(2, pw);
            students.set(id-1, student);
        }
        this.assign(this.sender());
        if (unassignedWork.isEmpty() && busyWorkers.isEmpty()){
            this.log.info("password cracking completed");

            createPrefixWork();
        }
    }

    private void handle(PrefixCompletionMessage message){
        this.log.info(message.toString());
        ArrayList<ArrayList<Integer>> combinations = message.combinations;
        if (prefixDone){
            busyWorkers.remove(this.sender());
            idleWorkers.add(this.sender());
            if (busyWorkers.isEmpty()) {
                createGeneWork();
            }
        }
        if (!combinations.isEmpty() && !prefixDone) {
            for (int i = 0; i < combinations.size(); i++) {
                for (int j = 0; j < combinations.get(i).size(); j++) {
                    students.get(j).add(String.valueOf(combinations.get(i).get(j)));
                }
            }
            this.log.info("finished prefix task");
            prefixDone = true;
            unassignedWork.clear();
            busyWorkers.remove(this.sender());
            idleWorkers.add(this.sender());
        } else {
            this.assign(this.sender());
        }

    }

    private void handle(GeneCompletionMessage message) {
        this.log.info(message.toString());
        int originId = message.originId;
        int partnerId = message.partnerId;
        int length = message.length;

        if (tempPartners.get(originId-1).get(1) < length){
            List<Integer> partner = new ArrayList<Integer>();
            partner.add(partnerId);
            partner.add(length);
            tempPartners.set(originId-1, partner);
        }
        this.assign(this.sender());
        if (unassignedWork.isEmpty() && busyWorkers.isEmpty()){
            for (int i = 0; i < students.size(); i++){
                students.get(i).add(Long.toString(tempPartners.get(i).get(0)));
            }
            this.log.info("finished gene task");
            createHashWork();
        }
    }

    private void handle(HashCompletionMessage message) {
        this.log.info(message.toString());
        hashValues.add(message.hash);
        this.assign(this.sender());
        if (hashValues.size()==students.size()){
            for (int i = 0; i < hashValues.size(); i++){
                ArrayList<String> hash = hashValues.get(i);
                students.get(Integer.parseInt(hash.get(0))-1).add(hash.get(1));
            }
            for (int j = 0; j < students.size(); j++){
                this.log.info(students.get(j).toString());
            }
            long duration = System.currentTimeMillis() - startTime;
            this.log.info("finished tasks in " + duration + " Milliseconds");
            while (!idleWorkers.isEmpty()) {
                ActorRef w = idleWorkers.poll();
                w.tell(PoisonPill.getInstance(), this.self());
            }
        }
    }

    private void createPrefixWork(){
        this.log.info("creating prefixWork");
        List<String> passwords = new ArrayList<String>();
        for (int j = 0; j < students.size(); j++){
            passwords.add(students.get(j).get(2));
        }
        double steps = Math.pow(2, students.size())-1;
        for (double i = 0; i < steps; i+=Math.floor(steps/5000)){
            long begin = (long)i;
            long range = (long) Math.floor(steps/5000);
            unassignedWork.add(new Worker.LinearCombinationWorkMessage(passwords, begin, range));
        }
        this.log.info("prefixWork created");
        this.assignAll();
    }

    private void createGeneWork() {
        this.log.info("start generating genework");
        for (int i = 0; i < students.size(); i++){
            for(int j = 0; j < students.size(); j+=4) {
                List<ArrayList<String>> studentPackage = students.subList(j, Math.min(j + 3, students.size() - 1)+1);
                List<ArrayList<String>> potentialPartners = new ArrayList<ArrayList<String>>();
                for (int k = 0; k < studentPackage.size(); k++){
                    ArrayList<String> partner = new ArrayList<>();
                    partner.add(studentPackage.get(k).get(0));
                    partner.add(studentPackage.get(k).get(3));
                    potentialPartners.add(partner);
                }
                unassignedWork.add(new Worker.GeneWorkMessage(i+1, students.get(i).get(3), potentialPartners));
            }
        }
        this.log.info("geneWork created");
        for (int j = 0; j < students.size(); j++){
            students.get(j).remove(3);
        }
        this.assignAll();
    }

    private void createHashWork() {
        this.log.info("creating HashWork");
        for(int j = 0; j <students.size(); j++) {
            String id = students.get(j).get(0);
            int prefix = Integer.parseInt(students.get(j).get(3));
            int partner = Integer.parseInt(students.get(j).get(4));
            unassignedWork.add(new Worker.HashMiningWorkMessage(id,prefix,partner));
        }
        this.log.info("HashWork created");
        this.assignAll();
    }

    private void assign() {
        WorkMessage work = this.unassignedWork.poll();
        ActorRef worker = this.idleWorkers.poll();
        if (work == null){
            return;
        }
        if (worker == null) {
            this.unassignedWork.add(work);
            this.log.info("no worker");
            return;
        }
        this.busyWorkers.put(worker, work);
        worker.tell(work, this.self());
    }

    private void assignAll() {
        int size = idleWorkers.size();
        for (int i = 0; i < size; i++){
            ActorRef worker = idleWorkers.poll();
            WorkMessage work = this.unassignedWork.poll();
            if (work == null) {
                this.idleWorkers.add(worker);
                return;
            }
            this.busyWorkers.put(worker, work);
            worker.tell(work, this.self());
        }
    }

    private void assign(ActorRef worker) {
        busyWorkers.remove(worker);
        WorkMessage work = this.unassignedWork.poll();
        if (work == null) {
            this.idleWorkers.add(worker);
            return;
        }
        this.busyWorkers.put(worker, work);
        worker.tell(work, this.self());
        this.log.info(Integer.toString(busyWorkers.size()));
        this.log.info(Integer.toString(idleWorkers.size()));
    }
}