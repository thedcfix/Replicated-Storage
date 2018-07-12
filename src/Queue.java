import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Queue {
	private String type;
	private List<Message> queue;
	private long cycle;
	private final Object lock = new Object();
	
	public Queue(String name) {
		type = name;
		queue = new ArrayList<>();
		cycle = 0;
	}
	
	public void add(Message m) {
		synchronized (lock) {
			// segno il ciclo in cui viene inserito in coda
			m.cycle = this.cycle;
			
			// aggiungo il messaggio
			queue.add(m);
			
			// ordino la coda
			Collections.sort(queue, (m1, m2) -> m1.source.hashCode() - m2.source.hashCode());
			Collections.sort(queue, (m1, m2) -> m1.lamport_clock - m2.lamport_clock);
		}
	}
	
	public void increaseCycle() {
		synchronized (lock) {
			cycle++;
		}
	}
	
	public Message getFirst() {
		
		synchronized(lock) {
			if (queue.size() != 0)
				return queue.get(0);
			else
				return null;
		}
	}
	
	public Message removeFirst() {
		
		@SuppressWarnings("unused")
		Message m;
		
		synchronized(lock) {
			if (!queue.isEmpty()) {
				m = queue.get(0);
				queue = queue.subList(1, queue.size());
				return m;
			}
			else
				return null;
		}
	}
	
	public List<Message> extractSublist(Message mess) {
		
		List<Message> msgs = new ArrayList<>();
		
		if (!queue.isEmpty()) {
			for(Message m : queue) {
				if (m.equalsUltraLite(mess)) {
					msgs.add(m);
				}
				else {
					break;
				}
			}
		}
		
		return msgs;
		
	}
	
	public void removeExecuted(Message mess) {
		
		synchronized (lock) {
			List<Message> toRemove = new ArrayList<>();
			
			for (Message m : queue) {
				if (m.equalsUltraLite(mess)) {
					toRemove.add(m);
				}
			}
			
			queue.removeAll(toRemove);
		}
	}
	
	public boolean isEmpty() {
		return queue.size() == 0;
	}
	
	public boolean isAlreadyPresent(Message msg) {
		boolean flag = false;
		
		for(Message m : queue) {
			if (msg.equals(m)) {
				flag = true;
				break;
			}
		}
		
		return flag;
	}
	
	public void makeValid(Message msg) {
		for(Message m : queue) {
			if (msg.equals(m)) {
				m.valid = true;
			}
		}
	}
	
	public void print() {
		System.out.println("\nCoda " + type + ": \n");
		
		if (!isEmpty()) {
			for (Message m : queue) {
				m.print();
			}
			
			System.out.println("\n");
		}
	}
	
	public void tick() {
		this.cycle++;
	}
	
	// se un messaggio è in coda da più di 2 cicli chiedo la ritrasmissione
	public int checkRetransmit() {
		
		if (!isEmpty()) {
			Message mes = getFirst();
			
			if (this.cycle >= mes.cycle + 2) {
				mes.cycle = this.cycle;
				return mes.getLightVersionHash();
			}
			else
				return -1;
			}
		else 
			return -1;
	}
}
