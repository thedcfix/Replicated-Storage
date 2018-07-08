import java.io.Serializable;

public class Message implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	public String type;
	public int lamport_clock;
	public int local_clock;
	public String source;
	public String sender;
	
	public int id;
	public int value;
	public int clientID;
	public boolean isAck;
	public boolean isRetransmit;
	public long cycle;
	public boolean valid;
	
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
		this.lamport_clock = m.lamport_clock;
		this.local_clock = m.local_clock;
		if(m.source != null)
			this.source = new String(m.source);
		if(m.sender != null)
			this.sender = new String(m.sender);
		this.id = m.id;
		this.value = m.value;
		this.clientID = m.clientID;
		this.isAck = m.isAck;
		this.isRetransmit = m.isRetransmit;
		this.cycle = m.cycle;
		this.valid = m.valid;
	}
	
	public boolean equals(Message m) {
		if (this.type.equals(m.type) && this.lamport_clock == m.lamport_clock && this.local_clock == m.local_clock && this.source.equals(m.source) && 
				this.sender.equals(m.sender) && this.id == m.id && this.value == m.value && this.clientID == m.clientID) {
			return true;
		}
		else {
			return false;
		}
	}
	
	public boolean equalsLite(Message m) {
		
		if (this.lamport_clock == m.lamport_clock && this.source.equals(m.source) && 
				this.id == m.id && this.value == m.value && this.clientID == m.clientID) {
			return true;
		}
		else {
			return false;
		}
	}
	
	public boolean equalsUltraLite(Message m) {
		
		if (this.source.equals(m.source) && this.id == m.id && this.value == m.value && this.clientID == m.clientID) {
			return true;
		}
		else {
			return false;
		}
	}
	
	public void print() {
		System.out.println("Messaggio: " + type + " " + id + " " + value + " da " + source + ". Lamport clock: " + lamport_clock + 
				". Local clock: " + local_clock + ". Ack source: " + sender + ". Is ack: " + isAck + ". Is valid: " + valid);
	}
	
	public int getLightVersionHash() {
		Message clone = new Message(this);
		
		clone.lamport_clock = 0;
		clone.local_clock = 0;
		clone.sender = "";
		clone.isAck = false;
		clone.isRetransmit = false;
		clone.cycle = 0;
		clone.valid = false;
		
		return clone.hashCode();
	}
}
