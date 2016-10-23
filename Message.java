import java.io.Serializable;

/**
 * Enumerated type to provide a semantic, abstracted way of representing the
 * messages sent between servers in 3PC. Has to implement Serializable so that
 * it can be pass through object streams */
public enum Message implements Serializable {
	QUERY,
	READY, UNABLE,
	PRE_COMMIT, ABORT,
	COMMIT,
	ACK_PRE_COMMIT, ACK_ABORT,
	ACK_FINAL
}
