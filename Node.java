/* PREAMBLE:
* Two Phase Commit coursework - I extended beyond the base spec and implemented
* the improved THREE phase commit instead.
*/
import dcs.os.Server;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
* @author Jarred Morris
* The main class that runs a particular server node instance
**/
public class Node {

	/**
	 * Method to print out to the run script how to use this programme through
	 * that script
	 **/
	public static void usage() {
		System.out.println("Usage:");
		System.out.println("Two program types: coordinator OR cohort...");

		System.out.println("coordinator:");
		System.out.println("\tjava -cp .:TPC.jar Node coordinator <coordinator port> <client port> <number of cohorts> <database path>");
		System.out.println("\t\t<coordinator port>: The port that this coordinator listens on for cohorts");
		System.out.println("\t\t<client port>: The port that this coordinator listens on for a client");
		System.out.println("\t\t<number of cohorts>: A value that dictates how many cohorts this coordinator will connect to");
		System.out.println("\t\t<database path>: The path to the database file for this server, this needs to be unique for each server");
		System.out.println("\t\tExamples of coordinator usage: ");
		System.out.println("\t\t\tCreate a coordinator that connects to 14 cohorts (ie: there will be 15 servers in total");
		System.out.println("\t\t\t\tjava -cp .:TPC.jar Node coordinator 9030 9050 14 db1.txt");

		System.out.println("OR");
		System.out.println("cohort:");
		System.out.println("\tjava -cp .:TPC.jar Node cohort <coordinator port> <database path>");
		System.out.println("\t\t<coordinator address>: The address of the coordinator that this cohort will connect to.");
		System.out.println("\t\t<database path>: The path to the database file for this server, this needs to be unique for each server");
		System.out.println("\t\tExamples of cohort usage: ");
		System.out.println("\t\t\tCreate a cohort that connects to the coordinator example above");
		System.out.println("\t\t\t\tjava -cp .:TPC.jar Node cohort localhost:9030 db2.txt");
		System.exit(1);
	}

	/**
	 * Main method; checks which type of srver to run this node as (cohort or
	 * coordinator and then calls appropriate method)
	 * @param args command line arguments
	 **/
	public static void main(String[] args) throws IOException {
		if (args.length < 1) {
			usage();
		}
		String program = args[0];
		switch (program) {
		case "coordinator": serverCoordinator(args); break;
		case "cohort": serverCohort(args); break;
		default:
			System.err.println("Unknown program type '" + program + "'");
			usage();
			break;
		}
	}

	/**
	 * Method to create a cohort server instances
	 * @param args command line arguments
	 **/
	private static void serverCohort(String[] args) throws IOException {
		if (args.length != 3) {
			usage();
		}

		// parse the address of the coordinator into singleton array
		InetSocketAddress[] coordAddress = Server.parseAddresses(args[1]);
		if (coordAddress.length != 1) {
			System.err.println("<cohort>> <Multiple coordinator addresses were supplied which should not have happenned, now killing this node>");
			System.exit(1);
		}
		String databasePath = args[2]; //grab db path from command line

		CohortServer cohort = new CohortServer(databasePath);

		//connect this cohort to the coord
		System.out.println("<cohort> <Connecting to coordinator>");
		cohort.connectServers(coordAddress);
		System.out.println("<cohort> <Successfully connected to coordinator>");

		//while cohort is running (not stopping), we must handle requests!
		while (!cohort.isStopping()) {
			try {
				System.out.println("<cohort> <Now ready for communications>");
				cohort.handleCoordinatorRequest();
			} catch (ClassNotFoundException e) {
				/* Shouldn't ever occur, we know that the objects being written to / read
				 * from streams is always of class type StockMessage, declared in this file */
				System.err.println("<cohort> {err] }<ClassNotFoundException occured. Killing this node and printing stack trace>");
				e.printStackTrace();
				cohort.setStopping();
			} catch (IOException e) {
				System.err.println("<cohort> {err} <IOException occured. Killing this node and printing stack trace>");
				e.printStackTrace();
				cohort.setStopping();
			}
		}

		//cohort will close if fatal error occurs that sets stopping
		cohort.close();
	}

	/**
	 * Method to create a coordinator server instances
	 * @param args command line arguments
	 **/
	private static void serverCoordinator(String[] args) throws IOException {
		if (args.length != 5) {
			/* Coordinator program must take 5 arguments */
			usage();
		}
		// The port that this server should listen on to accept connections from other servers
		int serverListenPort = Integer.parseInt(args[1]);

		// The port that this server should listen on to accept connections from clients
		int clientListenPort = Integer.parseInt(args[2]);

		int numOtherServers = Integer.parseInt(args[3]);
		String databasePath = args[4]; // The path to the database file
		CoordinatorServer coordinator = new CoordinatorServer(databasePath, numOtherServers);

		try {
			//Should probably call acceptClients() after connectServers()

			coordinator.acceptServers(serverListenPort);
			System.out.println("<coordinator> <Have successfully connected to all cohorts>");

			System.out.println("<coordinator> <About to begin listening out for client>");
			coordinator.acceptClients(clientListenPort);

			//keeping on waiting for next request while coordinating is running (NOT stopping)
			while (!coordinator.isStopping());

		} catch (IOException e) {
			System.err.println("<coordinator> {err} IOException occurred.>");
			e.printStackTrace(System.err);

		} finally {
			//never gets run as we want our server on 24/7
			coordinator.close();
		}
	}
}
