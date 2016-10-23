import dcs.os.Server;
import dcs.os.StockList;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

/**
 * @author Jarred Morris
 * Class to represent a cohort server in the 3PC protocol, extends the
 * server implementation
 **/
public class CohortServer extends Server {
	private Socket socket; //the socket for this cohort
	private boolean dbLocked; //a lock on its database or not

	/**
	 * Constructor takes in a path to the database file, calls Server constructor
	 * and locks the database
	 * @param databasePath a path to the database file
	 **/
	public CohortServer(String databasePath) throws IOException {
		//Call super class constructor (Server) and then unlock database file
		super(databasePath);
		dbLocked = false;
	}

	/**
	 * a method to listen and accept server connections
	 * @param port the port to listen on
	 **/
	@Override
	public void acceptServers(int port) throws IOException {
		throw new UnsupportedOperationException("Cohort nodes do not accept other server connections, they connect to a single coordinator via the connectServers(...) method. The acceptServers(...) functionality is implemented by CoordinatorServer.");
	}

	/**
	 * A method to connect this cohort to all other servers
	 * @param servers the addresses of the other servers to connect to
	 **/
	@Override
	public void connectServers(InetSocketAddress[] servers) throws IOException {
		/* Create two flags which indicate whether we are connected or timed out. Both initialised to false.
		 * Store the system time that this method began in milliseconds */
		boolean connected = false;
		boolean timedOut = false;
		long startTime = System.currentTimeMillis();

		/* We keep making attempts to connect to the server until timeout */
		while (!connected &&  !timedOut) {
			try {
				/* Create a socket connecting to the coordinator's address and port */
				socket = new Socket(servers[0].getAddress(), servers[0].getPort());

				/* If no exception has been thrown above then we are now connected */
				connected = true;
			} catch (ConnectException ce) {
				/* The connection failed. This is usually because this node tried to
				 * connect at the same time as another node. We pause for a second and
				 * then try again */
				try {
					Thread.sleep(1000);
					System.err.println("<cohort> <This is NOT usually a problem, but there was a ConnectException creating socket at " + servers[0].toString() + ". Trying again>");
					//ce.printStackTrace();
				} catch (InterruptedException ie) {
					/* Has to be caught in order to sleep this thread */
					System.err.println("<cohort> <InterruptedException thrown creating socket at " + servers[0].toString() + ". Trying again>");
					//ie.printStackTrace();
				}
			} finally {
				/* If 25000ms have passed since we first entered this method, then timeout */
				if (System.currentTimeMillis() - startTime > 25000) {
					timedOut = true;
				}
			}
		}

		if (!connected) {
			/* Equivalent to us having timed out above, kill this node */
			System.err.println("<cohort> <Timed out while connecting to Coordinator which is probably unreachable, this cohort node will now be killed>");
			close();
			System.exit(1);

		} else {
			/* Connection was made successfully */
			System.out.println("<cohort> <Successfully connected to Coordinator>");
		}
	}

	/**
	 * A method to close all connections and unlock this cohorts database
	 **/
	@Override
	public void close() throws IOException {
		//Call Server's close method, then close our own socket and unlock resources
		super.close();
		socket.close();
		dbLocked = false;
	}

	/**
	 * Method to handle a client stock request.
	 * @param stock the client's stock request
	 * @return true if the request succeeded or false if it failed.
	 **/
	@Override
	public boolean handleClientRequest(StockList stock) throws IOException {
		throw new UnsupportedOperationException("Cohort nodes do not connect to or handle requests from clients. The handleClientRequest(...) functionality is implemented by CoordinatorServer.");
	}

	/**
	 * Method to handle a request from this cohort's supervisor!
	 * @return true if the request succeeded or false if it failed.
	 **/
	public boolean handleCoordinatorRequest() throws IOException, ClassNotFoundException {

		try { //finally set dbLocked = false
			socket.setSoTimeout(0);
			ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream is = new ObjectInputStream(socket.getInputStream());

			/* ============== FIRST PHASE ============== */
			/* We are at the QUERY stage. Checking if we can commit or not *
			 * Write READY if we can commit, UNABLE else. */

			/* Wait for QUERY to begin, we don't timeout on this because we don't know
			 * when we'll get the *next* client's request. Instead, we can wait here */
			StockMessage stockMessage = (StockMessage)is.readObject();

			/* Query has begun, we want a timeout now */
			socket.setSoTimeout(15000);

			/* First check that correct message has arrived. If we have been asked to
			 * do something other than QUERY then a serious error has occured */
			if (stockMessage.getMessage() != Message.QUERY) {
				System.err.println("<cohort> <Invalid message type '" + stockMessage.getMessage() + "' received when only QUERY is allowed at this phase>");
				os.writeObject(new StockMessage(Message.UNABLE));
				return false;
			}

			/* Check if the database file exists before querying */
			if (!databaseExists()) {
				System.err.println("<cohort> <Database does not exist>");
				os.writeObject(new StockMessage(Message.UNABLE));
				return false;
			}

			/* Database exists so we are fine to continue. Get stock in database and
			 * the amount being requested.*/
			dbLocked = true;
			StockList stockRequested = stockMessage.getStock();
			StockList myStock = queryDatabase();

			/* If not enough stock then we send UNABLE (to commit), else we are READY */
			if (!myStock.enough(stockRequested)) {
				System.out.println("<cohort>> <Not enough stock, sending UNABLE status to coordinator>");
				os.writeObject(new StockMessage(Message.UNABLE));
			} else {
				System.out.println("<cohort> <Enough stock available, sending READY status to coodinator>");
				os.writeObject(new StockMessage(Message.READY));
			}


			/* ============== SECOND PHASE ============== */
			/* We are at the PRE_COMMIT or ABORT stage. Checking if we can commit or not *
			 * Return READY if we can commit, UNABLE else. */

			try {
				 stockMessage = (StockMessage)is.readObject();

			} catch (SocketTimeoutException e) {
				System.err.println("<cohort> <Timed out waiting for whether to PRE_COMMIT or ABORT - forced to assume ABORT");
				return false;
			}

			/* If we get here then no timeout occured and we have a message telling us
			 * either to PRE_COMMIT or to ABORT */

			Message msgForCoord;
			if (stockMessage.getMessage() == Message.PRE_COMMIT) {
				msgForCoord = Message.ACK_PRE_COMMIT;
				System.out.println("<cohort> <Instructed to PRE_COMMIT>");

			} else if (stockMessage.getMessage() == Message.ABORT) {
				System.out.println("<cohort> <Instructed to ABORT>");
				msgForCoord = Message.ACK_ABORT;

			} else {
				System.err.println("<cohort> <Unexpected message of " + stockMessage.getMessage() + " received when expected only PRE_COMMIT or ABORT here, assuming ABORT>");
				msgForCoord = Message.ACK_ABORT;
			}

			//We write back our acknowledgement

			os.writeObject(new StockMessage(msgForCoord));
			if (msgForCoord == Message.ACK_ABORT) {
				System.err.println("<cohort> <ABORT acknowledged, the transaction will not occur>");
				return false;
			}


			/* ============== THIRD PHASE ============== */
			/* We are at the COMMIT or ABORT phase. We now have PRE-COMMITted so we
			 * know that every node has enough stock, we will only abort here if
			 * a cohort goes down */

			Message response;
			try {
				response = ((StockMessage)is.readObject()).getMessage();
			} catch (SocketTimeoutException e) {
				/* Timed out waiting for COMMIT/ABORT */
				System.out.println("<cohort> <Timed out waiting for COMMIT but this node has already PRE_COMMITted so will now DO this COMMIT>");
				//DO COMMIT
				response = Message.COMMIT;
			}
			boolean succeeded = true;
			if (response == Message.ABORT) {
				System.out.println("<cohort> <Doing ABORT>");
				succeeded = false;

			} else if (response == Message.COMMIT) {
				//DO COMMIT
				System.out.println("<cohort> <Received COMMIT, now committing changes to this database>");
				myStock.remove(stockRequested);
				writeDatabase(myStock);

			} else {
				System.err.println("<cohort> <Unexpected message of " + stockMessage.getMessage() + " received when expected only COMMIT or ABORT here, assuming ABORT>");
				succeeded = false;
			}

			os.writeObject(new StockMessage(Message.ACK_FINAL));

			return succeeded;

		} finally { //always unlock the database when we are done!
			dbLocked = false;
		}
	}
}
