import java.util.List;

public class SharedContent {
	
	private List<Message> queue;
	private boolean validity;
	
	public SharedContent(List<Message> queue) {
		this.queue = queue;
	}
	
	public SharedContent(boolean value) {
		validity = value;
	}

	public List<Message> getQueue() {
		return queue;
	}

	public void setQueue(List<Message> queue) {
		this.queue = queue;
	}

	public boolean getValidity() {
		return validity;
	}

	public void setValidity(boolean validity) {
		this.validity = validity;
	}

}
