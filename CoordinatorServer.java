import dcs.os.Server;
import dcs.os.StockList;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;


/**
 * @author Jarred Morris
 * Class to represent a coordinating server in the 3PC protocol, extends the
 * server implementation
 **/
public class CoordinatorServer extends Server {
	private boolean dbLocked; //resource lock  - true <=> database is locked
	private ServerSocket serverSocket; //to create sockets from
	private Socket[] sockets; //connection to cohorts

	/**
	 * Constructor to create a coordinator
	 * @param databasePath a path to the database file
	 * @param numOtherServers the number of other servers (cohorts)
	 **/
	public CoordinatorServer(String databasePath, int numOtherServers) throws IOException {
		/* Call super constructor and then create a socket for each cohort, don't lock databases yet */
		super(databasePath);
		sockets = new Socket[numOtherServers];
		dbLocked = false;
	}

	/**
	 * A method to listen and accept server connections
	 * @param port the port to listen on
	 **/
	@Override
	public void acceptServers(int port) throws IOException {
		/* Listen on this port for cohorts */
		serverSocket = new ServerSocket(port);

		System.out.println("<coordinator> <Now accepting cohorts>");

		for (int i = 0; i < sockets.length; i++) {
			/* Create a socket for each cohort */
			sockets[i] = serverSocket.accept();
			System.out.println("<coordinator> <successfully connected to cohort on socket[" + i + "]");
		}
	}

	/**
	 * Method to perform preliminary checks on the server. Locks database where needed
	 * @return true if the database provided actually exists, is not locked, has
	 * enough stock to service the query. Else returns false.
	 * @param stock the stocklist being requested
	 **/
	private boolean prelimChecks(StockList stock) throws IOException {
		if (!databaseExists()) {
			/* Coordinator doesn't have a database! Unlikely to ever occur but abort now */
			System.err.println("<coordinator> {err} <No database file!>");
			return false;
		}

		if (dbLocked) {
			/* Resources are locked so another client must be doing something, abort */
			System.err.println("<coordinator> {err} <Database is locked, this means a communication is already happening with another client, perhaps try again later>");
			return false;
		}

		//Get stock in database
		StockList myStock = queryDatabase();
		//Lock the resources now
		dbLocked = true;

		if (!myStock.enough(stock)) {
			/* If coordinator doesn't have enough stock, there's no point even talking
			 * to the cohorts, we'll need to abort regardless */
			System.out.println("<coordinator> <NOT enough stock in coordinator's database, aborting transaction without communicating to cohorts>");
			return false;
		}

		/* We passed the preliminary checks so return true!*/
		return true;
	}

	/**
	 * Method to perform the first phase of the 3PC protocol
	 * @param handlers An array list of the socket handlers for coordinator
	 * @param pool the executor service to thread this application
	 * @return Message.ABORT if response unable from any cohort, else it will
	 * return Message.PRE_COMMIT.
	 **/
	private Message firstPhase(ArrayList<CoordinatorSocketHandler> handlers, ExecutorService pool) throws IOException {
		/* Make the communication with cohorts and await response */

		Message nextPhase = Message.PRE_COMMIT; //Initialise to PRE_COMMIT. If we can't PRE_COMMIT it gets changed
		List<Future<Message>> futures = null; //Waits until a Callable is done and gives us the result
		Iterator<Future<Message>> it = null; //To iterate over the futures


		try {
			//Invoke all of the threads, block until done and return result into future
			//We invoke all because we need to know the result from each cohort before making a decision
			futures = pool.invokeAll(handlers);
			it = futures.iterator();
			while (it.hasNext()) {
				/* Look at each cohort response. If even one is UNABLE then we set
				 * nextPhase to UNABLE. Else it will stay at its initial value of PRE_COMMIT */
				Message response = it.next().get();

				if (response == Message.UNABLE) {
					// If any cohort was unable it either lacks stock or died/timed out so we ABORT
					nextPhase = Message.ABORT;

				} else if (response != Message.READY) {
					//If any cohort responsed with a message other than UNABLE/READY then something went wrong so we assume UNABLE
					System.err.println("<coordinator> {err} <Expected either UNABLE or READY from cohort but got " + response + ">");
					nextPhase = Message.ABORT;
				}
			}
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		//Returning either PRE_COMMIT or ABORT
		return nextPhase;
	}

	/**
	 * Method to perform the second phase of the 3PC protocol
	 * @param handlers An array list of the socket handlers for coordinator
	 * @param pool the executor service to thread this application
	 * @return Message.ACK_ABORT if a single cohort did not precommit, else all
	 * cohorts did precommit and so we return Message.COMMIT to commit changes to
	 * databases at each node
	 **/
	private Message secondPhase(ArrayList<CoordinatorSocketHandler> handlers, ExecutorService pool) throws IOException {
		//See firstPhase for exaplanation of these variables
		List<Future<Message>> futures = null;
		Iterator<Future<Message>> it = null;
		Message nextPhase = null;
		try {
			//Invoke all of the threads, block until done and return result into future
			futures = pool.invokeAll(handlers);
			it = futures.iterator();

			//For each future, do
			while (it.hasNext()) {
				Message response = it.next().get();

				if (response == Message.ACK_ABORT) {
					//If cohort did not acknowledge precommit then we must abort everywhere
					System.out.println("<coordinator> <ABORT requested>");
					nextPhase = Message.ACK_ABORT;
					break;

				} else if (response == Message.ACK_PRE_COMMIT) {
					//cohort acknowledged the precommit so it's time to do commit!
					nextPhase = Message.COMMIT;

				} else {
					//If cohort got a message other than ACK/DID_NOT_ACK then something serious went wrong and we abort
					System.err.println("<coordinator> {err} <Expected either ACK_COMMIT or ACK_ABORT from cohort but got " + response + ". Assuming ABORT>");
					nextPhase = Message.ACK_ABORT;
				}
			}
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return nextPhase;
	}

	/**
	 * Method to perform the third phase of the 3PC protocol
	 * @param handlers An array list of the socket handlers for coordinator
	 * @param pool the executor service to thread this application
	 * @param myStock Stock at this node
	 * @param stock Stock request
	 * @param previousPhase the message from the previous phase
	 * @return true if transaction was a success over all nodes, false else
	 **/
	private boolean finalPhase(ArrayList<CoordinatorSocketHandler> handlers, ExecutorService pool, StockList myStock, StockList stock, Message previousPhase) throws IOException {
		//See firstPhase function for better explanation of variables
		List<Future<Message>> futures = null;
		Iterator<Future<Message>> it = null;
		Message nextPhase = previousPhase;

		try {
			//Invoke all of the threads, block until done and return result into future
			futures = pool.invokeAll(handlers);
			it = futures.iterator();

			try {
				while (it.hasNext()) {
					Message response = it.next().get();
					if (response != Message.ACK_FINAL) {
						//If we didn't get the ACK then some sort of ABORT must have happened
						nextPhase = Message.ABORT;
					}
					//System.err.println(it.next().get());
				}
			} catch (ExecutionException e) {
				System.err.println("<coordinator> {err} <ExecutionException occured, now printing stack trace");
				e.printStackTrace();
			}

			if (nextPhase == Message.COMMIT) {
				/* We got the ACK from the cohort that the COMMIT went ahead, finish up by changing our database */
				System.out.println("<coordinator> <Third phase done: Received ACK that COMMIT was successful on cohorts, now will write changes to my database>");
				myStock = queryDatabase();
				myStock.remove(stock);
				writeDatabase(myStock);
				return true; //request for stock was successful

			} else if (nextPhase == Message.ABORT) {
				System.out.println("<coordinator> <Final phase ABORT happened, no transaction occured>");

			} else {
				System.err.println("<coordinator> {err} <Expected either ACK_FINAL or ABORT from cohort but got " + nextPhase + ". Will assume ABORT>");
			}

		} catch (InterruptedException e) {
			System.err.println("<coordinator> {err} <InterruptedException occurred. Now printing stack trace>");
			e.printStackTrace();
		}

		return false;
	}

	/** Method to handle a request from a client.
	 * @param stock the stock being requested by client
	 * @return true if the request succeeded or false if it failed.
	 **/
	@Override
	public boolean handleClientRequest(StockList stock) throws IOException {

		try { //finally unlocks the database

			/* First we handle a few outlier cases (eg: resource is locked, etc) */
			if (!prelimChecks(stock)) {
				return false;
			}

			StockList myStock = queryDatabase();

			if (sockets.length == 0 ) {
				/* Only one server was set up in the runServers.sh script. Ie: there are no cohort */
				/* We know from above that there is enough stock, so let's do the transaction */
				myStock.remove(stock);
				writeDatabase(myStock);
				return true;
			}

			/* Creating streams and threadpools */
			ObjectOutputStream[] os = new ObjectOutputStream[sockets.length];
			ObjectInputStream[] is = new ObjectInputStream[sockets.length];
			ExecutorService pool = Executors.newFixedThreadPool(16);
			ArrayList<CoordinatorSocketHandler> handlers = new ArrayList<CoordinatorSocketHandler>(sockets.length);

			List<Future<Message>> futures = null;
			Iterator<Future<Message>> it = null;

			for (int i = 0; i < sockets.length; i++) {
				/* Initialise streams */
				os[i] = new ObjectOutputStream(sockets[i].getOutputStream());
				is[i] = new ObjectInputStream(sockets[i].getInputStream());
			}

			/******* FIRST PHASE *******/
			/***************************/

			/* We make QUERY (to commit) to cohorts */
			StockMessage stockMessage = new StockMessage(stock, Message.QUERY);

			for (int i = 0; i < sockets.length; i++) {
				/* Create our callables that we invoke later */
				handlers.add(new CoordinatorSocketHandler(sockets[i], stockMessage, os[i], is[i]));
			}

			// We perform the first phase, which returns either PRE_COMMIT or ABORT
			Message nextPhase = firstPhase(handlers, pool);
			System.out.println("<coordinator> <First phase complete, cohort voted for " + nextPhase +">");


			/******* SECOND PHASE *******/
			/* We send PRE_COMMIT or ABORT to cohorts */

			for (int i = 0; i < sockets.length; i++) {
				//Set each handler to a new instance containing the new message

				CoordinatorSocketHandler csh = handlers.get(i);
				csh.updateMessage(nextPhase);
				handlers.set(i,csh);
			}

			nextPhase = secondPhase(handlers, pool);
			/* Second phase done, nextPhase enum is either COMMIT or ACK_ABORT */

			if (nextPhase == Message.ACK_ABORT) {
				System.out.println("<coordinator> <Second phase complete, cohort have sent ACK_ABORT to acknowledge that the ABORT was a success>");
				return false;
			}

			//If we get here, nextPhase is COMMIT
			System.out.println("<coordinator> <Second phase complete, cohort voted for " + nextPhase +">");

			/******* THIRD PHASE *******/
			/* We make COMMIT or ABORT to cohorts */
			for (int i = 0; i < sockets.length; i++) {
				//Set each handler to a new instance containing the new message
				CoordinatorSocketHandler csh = handlers.get(i);
				csh.updateMessage(nextPhase);
				handlers.set(i,csh);
				//handlers.set(i, new CoordinatorSocketHandler(sockets[i], stockMessage, os[i], is[i]));
			}

			return finalPhase(handlers, pool, myStock, stock, nextPhase);

		} finally {
			//Always executes before any return in this function, unlocks resources
			dbLocked = false;
		}
	}

/**
 * Close all connections, unlock databases
 **/
	@Override
	public void close() throws IOException {
		//Call close on super class (Server) and then close each socket, plus the server socket
		//Conclude by unlocking resource
		super.close();
		for (int i = 0; i < sockets.length; i++) {
			sockets[i].close();
		}
		serverSocket.close();
		dbLocked = false;
	}

	/**
	 * Method to connect to all other servers
	 * @param servers addresses to connect to each other server
	 **/
	@Override
	public void connectServers(InetSocketAddress[] servers) throws IOException {
		throw new UnsupportedOperationException("Coordinator node does not connect servers, it accepts connections from cohort nodes on the acceptServers(...) method. The connectServers(...) functionality is implemented by CohortServer.");
	}
}
