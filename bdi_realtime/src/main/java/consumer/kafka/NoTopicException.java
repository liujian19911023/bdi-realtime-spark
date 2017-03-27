package consumer.kafka;

public class NoTopicException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public NoTopicException(String msg){
		super(msg);
	}

}
