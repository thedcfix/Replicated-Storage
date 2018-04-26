import java.util.Hashtable;
import java.util.List;

public class SharedContent {
	
	private List<Message> queue;
	private boolean validity;
	private Hashtable<Integer, Integer> storage;
	
	public SharedContent(List<Message> queue) {
		this.queue = queue;
	}
	
	public SharedContent(boolean value) {
		validity = value;
	}
	
	public SharedContent(Hashtable<Integer, Integer> storage) {
		this.storage = storage;
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

	public Hashtable<Integer, Integer> getStorage() {
		return storage;
	}

	public void setStorage(Hashtable<Integer, Integer> storage) {
		this.storage = storage;
	}

}
