package de.hpi.ddm1HenschelClasen;

import com.typesafe.config.Config;

import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.ddm1HenschelClasen.actors.Reaper;
import de.hpi.ddm1HenschelClasen.actors.Worker;
import de.hpi.ddm1HenschelClasen.actors.listeners.MetricsListener;

public class Ddm1HenschelClasenSlave extends Ddm1HenschelClasenSystem {

	public static final String SLAVE_ROLE = "slave";
	
	public static void start(String actorSystemName, int workers, String host, int port, String masterHost, int masterPort) {
		
		final Config config = createConfiguration(actorSystemName, SLAVE_ROLE, host, port, masterHost, masterPort, workers);
		
		final ActorSystem system = createSystem(actorSystemName, config);
		
		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {
				system.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);
				//system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
				system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);

				for (int i = 0; i < workers; i++)
					system.actorOf(Worker.props(), Worker.DEFAULT_NAME + i);
			}
		});
	}
}
