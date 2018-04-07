import java.util.List;

public class SharedContent {
	
	private List<Message> queue;
	
	public SharedContent(List<Message> queue) {
		this.queue = queue;
	}

	public List<Message> getQueue() {
		return queue;
	}

	public void setQueue(List<Message> queue) {
		this.queue = queue;
	}

}
