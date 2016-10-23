import dcs.os.StockList;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.Callable;

/**
 * @author Jarred Morris
 * Class to handle a coordinator socket thread; more specifically is the
 * Callable utlised by the thread pool. See CoordinatorServer and CohortServer.
 **/
public class CoordinatorSocketHandler implements Callable<Message> {
	private Socket socket; //socket this thread is using for comms
	private ObjectOutputStream os;
	private ObjectInputStream is;
	private StockList stock;
	private Message message;

	/**
	 * Constructor takes in parameters for this handler, described in following
	 * doc remarks;
	 * @param socket the socket this thread is based upon
	 * @param stockMessage the stock request message to be services
	 * @param os the output stream for this thread to write out to
	 * @param is the input stream for this thread to read in from
	 **/
	public CoordinatorSocketHandler(Socket socket, StockMessage stockMessage, ObjectOutputStream os, ObjectInputStream is) {

		this.socket = socket;
		this.stock = stockMessage.getStock(); //get the stock list from the message
		this.message = stockMessage.getMessage(); //get the 3PC message
		this.os = os;
		this.is = is;
	}

	/**
	 * Call method, invoked and then returns a Message. See the Callable interface.
	 * Here we yield the responses made at each phase
	 **/
	@Override
	public Message call() throws IOException, ClassNotFoundException {

		Message response;
		try {
			os.writeObject(new StockMessage(stock, message));
			os.flush();

			socket.setSoTimeout(15000);
			return ((StockMessage)is.readObject()).getMessage();

		} catch (SocketTimeoutException e) {
			/* A timeout occured. This means we have to make some sort of decision About
			 * what to do next! See the case analysis below the switch statement for explanation */

			System.err.println("<coordinator> <Timeout occurred, handling this now>");

			switch (this.message) {
			case QUERY: return Message.UNABLE;
			case PRE_COMMIT: return Message.ABORT;
			case COMMIT: return Message.COMMIT;
			case ABORT: return Message.ABORT;
			default: System.err.println("<FATAL ERROR> <Invalid phase!>"); return null;
			}
			/* Case analysis:
			 * If we had no response from the cohort when it received a message of:
			 *  QUERY, then this cohort is in the UNABLE state (being dead is equivalent to not having stock)
			 *  PRE_COMMIT, then the entire transaction must be ABORTed.
			 *  COMMIT, then this cohort has already been PRE_COMMITed, we continue the COMMIT
			 *  ABORT, we must continue ABORT on all other cohorts*/
		}
	}

	/**
	 * Setter for the message
	 * @param message the message we want to update this handler to use
	 **/
	public void updateMessage(Message message) {
		/* Allows us to update the message in subsequent phases without having to
 	 	 * reinstantiate a thread because that would be VERY expensive!*/
		this.message = message;
	}
}
