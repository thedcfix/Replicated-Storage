import java.io.Serializable;

public class Message implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	public String type;
	public int clock;
	public String source;
	public String ackSource;
	public int id;
	public int value;
	public int clientID;
	public boolean isAck;
	public boolean isRetransmit;
	public long cycle;
	public boolean requestedAck;
	public boolean hasPrevious;
	public boolean executable;
	
	public Message(String type) {
		this.type = type;
	}
	
	public Message(String type, int id) {
		this.type = type;
		this.id = id;
	}
	
	public Message(String type, int id, int value) {
		this.type = type;
		this.id = id;
		this.value = value;
	}
	
	public Message(Message m) {
		this.type = m.type;
		this.clock = m.clock;
		if(m.source != null)
			this.source = new String(m.source);
		if(m.ackSource != null)
			this.ackSource = new String(m.ackSource);
		this.id = m.id;
		this.value = m.value;
		this.clientID = m.clientID;
		this.isAck = m.isAck;
		this.isRetransmit = m.isRetransmit;
		this.cycle = m.cycle;
		this.requestedAck = m.requestedAck;
		this.hasPrevious = m.hasPrevious;
		this.executable = m.executable;
	}
	
	public boolean equals(Message m) {
		
		if (this.type.equals(m.type) && this.clock == m.clock && this.source.equals(m.source) && 
				this.id == m.id && this.value == m.value && this.clientID == m.clientID) {
			return true;
		}
		else {
			return false;
		}
	}
	
	public boolean equals(Message m, String mode) {
		
		if (this.type.equals(m.type) && this.clock == m.clock && this.source.equals(m.source) && 
				this.ackSource.equals(m.ackSource) && this.id == m.id && this.value == m.value && 
					this.clientID == m.clientID) {
			return true;
		}
		else {
			return false;
		}
	}
	
	public boolean equalsLite(Message m) {
		
		if (this.clock == m.clock && this.source.equals(m.source) && 
				this.id == m.id && this.value == m.value && this.clientID == m.clientID) {
			return true;
		}
		else {
			return false;
		}
	}
	
	// serve per controllare se esiste un messaggio precedente a quello fornito come parametro
	public boolean equalsPrevious(Message m) {
		if (this.clock == (m.clock - 1) && this.source.equals(m.source)) {
			return true;
		}
		else {
			return false;
		}
	}
	
	public void print() {
		System.out.println("Messaggio: " + type + " " + id + " " + value + " da " + source + ". Clock: " + clock + ". Ack source: "  + 
				ackSource + ". Is ack: " + isAck + ". Is retransmission: " + isRetransmit + ". Is executable: " + executable);
	}
}
