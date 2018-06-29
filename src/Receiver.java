import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

public class Receiver extends Thread {
	
	private InetAddress group;
	private int SERVERS_PORT;
	private MulticastSocket multicast;
	public String IP;
	public Hashtable<Integer, ObjectOutputStream> usersTable;
	
	private Queue queue;
	private Queue ack;
	
	private List<Message> ackList;
	private Hashtable<Integer, Integer> storage;
	private List<Message> receivedMessages;
	
	public Set<String> servers;
	public SharedContent valid;
	public SharedContent db;
	
	public int DELIVERY_PORT = 8503;

	public Receiver(int port, Hashtable<Integer, ObjectOutputStream> users, Set<String> otherServers, Queue queue) throws IOException {
		
		SERVERS_PORT = port;
		usersTable = users;
		servers = otherServers;
		
		group = InetAddress.getByName("224.0.5.1");
		multicast = new MulticastSocket(SERVERS_PORT);
		multicast.joinGroup(group);		
		IP = InetAddress.getLocalHost().getHostAddress();
		
		this.queue = queue;
		this.ack = new Queue("ack");
		
		ackList = new ArrayList<>();
		storage = new Hashtable<>();
		receivedMessages = new ArrayList<>();
		valid = new SharedContent(false);
		db = new SharedContent(storage);
		
		new AlivenessSender().start();
		new AlivenessChecker(SERVERS_PORT, servers, valid).start();
		new StorageUpdater(db).start();
	}
	
	// ricevo i messaggi inviati sulla SERVERS_PORT
	public Message receiveMessage(byte[] buff) throws IOException, ClassNotFoundException {
		DatagramPacket recv = new DatagramPacket(buff, buff.length);
		multicast.receive(recv);
		
		ByteArrayInputStream bais = new ByteArrayInputStream(buff);
		ObjectInputStream ois = new ObjectInputStream(bais);
		
		Message mess = (Message) ois.readObject();
		
		return mess;
	}
	
	// manda un messaggio in multicast
	public void sendMulticast(Message msg) throws IOException, InterruptedException {
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream(6400);
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(msg);
		byte[] data = baos.toByteArray();
		
		DatagramPacket packet = new DatagramPacket(data, data.length, group, SERVERS_PORT);
		
		multicast.send(packet);
	}
	
	// verifico se un messaggio ha tutti gli ack
	public boolean isFullyAcknowledged(Message msg, Queue ack) {
		
		if (msg == null)
			return false;
		
		if (ack.extractSublist(msg).size() >= servers.size())
			return true;
		else
			return false;
	}
	
	public void run() {
		
		byte[] buff = new byte[8192];
		String sender;
		int lastClock;
		Hashtable<String, Integer> knownServers = new Hashtable<>();
		
		// lista contenente tutti i messaggi eseguiti
		List<Message> executionList = new ArrayList<>();
		
		while(true) {
			
			try {
				// ricevo il messaggio
				Message mess = receiveMessage(buff);
				
				if (!mess.type.equals("unlock")) {
					
					// estraggo il mittente del messaggio
					sender = mess.sender;
					
					// se il mittente non è conosciuto, lo inizializzo. Imposto il suo ultimo local clock a 0, così facendo mi aspetto che
					// il prossimo sia il suo primo messaggio con local clock 1
					if (knownServers.containsKey(sender) == false) {
						knownServers.put(sender, 0);
						lastClock = 0;
					}
					else {
						lastClock = knownServers.get(sender);
					}
					
					// marco il messaggio come valido o non valido
					if (mess.local_clock == lastClock + 1)
						mess.valid = true;
					else
						mess.valid = false;
					
					// ora controllo se è un ack o un messaggio
					if(mess.isAck == false) {
						// se non è già presente lo aggiungo (poi pulisco nel caso sia già stato processato in passato)
						if(!queue.isAlreadyPresent(mess)) {
							queue.add(mess);
						}
						else if (mess.valid){
							// se il messaggio è valido e ne ho una copia in coda che non lo è, la rendo valida
							queue.makeValid(mess);
						}
						
						/*
						 * 
						 * Inserire la parte di risposta alle read
						 * 
						 */
						
						// rispondo col mio ack
						Message ackMsg = new Message(mess);
						
						ackMsg.isAck = true;
						ackMsg.sender = IP;
						ackMsg.valid = false;
						
						// invio il mio messaggio di ack
						if (!mess.type.equals("read")) {
							sendMulticast(ackMsg);
						}
					}
					else {
						// se non è già presente lo aggiungo
						if (!ack.isAlreadyPresent(mess)) {
							
							// marco il messaggio come valido o non valido
							if (mess.local_clock == lastClock + 1)
								mess.valid = true;
							else
								mess.valid = false;
							
							// aggiungo l'ack alla lista delgi ack ricevuti
							ack.add(mess);
						}
					}
					
					queue.print();
					ack.print();
					
					// separatore iterazioni
					System.out.println("\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n");
					
					if (isFullyAcknowledged(queue.getFirst(), ack)) {
						Message toExecute = queue.removeFirst();
						ack.remove(toExecute);
						storage.put(toExecute.id, toExecute.value);
						
						
						
						System.out.println("Messaggio eseguito: " + storage.toString());
					}
					
					
					// separatore iterazioni
					System.out.println("\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n");
				}
				
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
