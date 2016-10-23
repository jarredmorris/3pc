import java.io.Serializable;
import dcs.os.StockList;

/**
 * @author Jarred Morris
 * A class representing a different kind of message to be sent between servers.
 * It stores a StockList and a Messages together so as to provide a means for
 * making a stock request between servers! */
public class StockMessage implements Serializable {
	private StockList stock; //the stock being requested; null means no stock requested
	private Message message; //the message being sent between servers

	/**
	 * Constructor to send a message with a stock request
	 * @param stock the stock request being made
	 * @param message the message being sent between servers
	 **/
	public StockMessage(StockList stock, Message message) {
		this.stock = stock;
		this.message = message;
	}

	/**
	 * Constructor to send a message without ANY stock request. Sometimes we want
	 * a StockMessage just to box up a Message, so that we don't have to worry
	 * about types during stream read/writes. Hence we can always send a
	 * StockMessage even if stock is not being requested, and then check the
	 * message and for nullity of the StockList. Thus constructor let's us do
	 * that by setting the stock to null
	 * @param message the message being sent between servers
	 **/
	public StockMessage(Message message) {
		/* S */
		this.stock = null;
		this.message = message;
	}

	/**
	 * Getter for the stocklist in this message
	 * @return the stock list being requested in this message, null means no
	 * stock is being requested from the recipient, but a message still being sent
	 **/
	public StockList getStock() {
		return stock;
	}

	/**
	 * Getter for the enumerated message in this message
	 * @return the message being sent to the recipient server
	 **/
	public Message getMessage() {
		return message;
	}

	/**
	 * Get string representation of message. It's not particularly useful to
	 * print out the stock since we usually know what it is prior to construction.
	 * Hence this just prints the Message
	 * @return a string representation of the message but WITHOUT any detail on
	 * the stock itself */
	public String toString() {
		return message.toString();
	}
}
