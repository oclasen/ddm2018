package de.hpi.octopus;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.octopus.actors.Profiler;
import de.hpi.octopus.actors.Worker;
import de.hpi.octopus.actors.listeners.ClusterListener;

public class OctopusMaster extends OctopusSystem {

	public static final String MASTER_ROLE = "master";

	public static void start(String actorSystemName, int workers, int slaves, String host, int port, String path) {

		final Config config = createConfiguration(actorSystemName, MASTER_ROLE, host, port, host, port, workers);
		
		final ActorSystem system = createSystem(actorSystemName, config);
		
		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {
				system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
			//	system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);

				system.actorOf(Profiler.props(), Profiler.DEFAULT_NAME);
				
				for (int i = 0; i < workers; i++)
					system.actorOf(Worker.props(), Worker.DEFAULT_NAME + i);

				String line;
				BufferedReader br;
				ArrayList<ArrayList<String>> students = new ArrayList<ArrayList<String>>();
				try {
					br = new BufferedReader(new FileReader(path));
					br.readLine(); // this will read the first line
					line = null;
					int i = 0;
					while ((line = br.readLine()) != null) {

//				use semicolon as separator
						String[] values = line.split(";");
						students.add(new ArrayList<String>(Arrays.asList(values)));

					}
				} catch (IOException e){
					e.printStackTrace();
				}

				system.actorSelection("/user/" + Profiler.DEFAULT_NAME).tell(new Profiler.TaskMessage(students), ActorRef.noSender());
				
			//	int maxInstancesPerNode = workers; // TODO: Every node gets the same number of workers, so it cannot be a parameter for the slave nodes
			//	Set<String> useRoles = new HashSet<>(Arrays.asList("master", "slave"));
			//	ActorRef router = system.actorOf(
			//		new ClusterRouterPool(
			//			new AdaptiveLoadBalancingPool(SystemLoadAverageMetricsSelector.getInstance(), 0),
			//			new ClusterRouterPoolSettings(10000, workers, true, new HashSet<>(Arrays.asList("master", "slave"))))
			//		.props(Props.create(Worker.class)), "router");
			}
		});

	}
}
