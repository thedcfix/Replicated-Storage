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
import java.util.HashSet;
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
	
	private Server server;
	Hashtable<String, Integer> progressTable;

	public Receiver(int port, Hashtable<Integer, ObjectOutputStream> users, Set<String> otherServers, Queue queue, Server server, 
			Hashtable<String, Integer> progressTable) throws IOException {
		
		SERVERS_PORT = port;
		usersTable = users;
		
		group = InetAddress.getByName("224.0.5.1");
		multicast = new MulticastSocket(SERVERS_PORT);
		multicast.joinGroup(group);		
		IP = InetAddress.getLocalHost().getHostAddress();
		
		//servers = otherServers; -----------------------------------------------
		servers = new HashSet<>();
		servers.add("192.168.1.176");
		servers.add("192.168.1.222");
		
		this.queue = queue;
		this.ack = new Queue("ack");
		
		this.server = server;
		
		ackList = new ArrayList<>();
		storage = new Hashtable<>();
		receivedMessages = new ArrayList<>();
		valid = new SharedContent(false);
		db = new SharedContent(storage);
		
		new AlivenessSender().start();
		//new AlivenessChecker(SERVERS_PORT, servers, valid).start();
		new StorageUpdater(db).start();
		
		progressTable = new Hashtable<>();
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
	public boolean isFullyAcknowledged(Message msg, Queue ack) throws InterruptedException {
		
		if (msg == null)
			return false;
		
		int size = this.servers.size();

		List<Message> ackMsgs = ack.extractSublist(msg);
		boolean flag = true;
		
		// vedo se tutti gli ack sono messaggi validi
		for (Message m : ackMsgs) {
			if (m.valid == false) {
				flag = false;
				break;
			}
		}
		
		// eseguo solo se ho tutti gli ack e se tutti gli ack sono validi
		if (ackMsgs.size() >= size && flag == true) {
			System.out.println("Servers size: " + size + " flag = " + flag + " EXECUTION ALLOWED");
			return true;
		}
		else
			return false;
	}
	
	public void run() {
		
		byte[] buff = new byte[8192];
		String sender;
		int expectedClock;
		Hashtable<String, Integer> knownServers = new Hashtable<>();
		Hashtable<Integer, Message> sentAcks = new Hashtable<>();
		
		// lista contenente tutti i messaggi eseguiti
		List<Message> executionList = new ArrayList<>();
		
		while(true) {
			
			try {
				// ricevo il messaggio
				Message mess = receiveMessage(buff);
				
				if (mess.isRetransmit == true) {
					// gestione ritrasmisisone. Uso il campo id per la trasmissione dell'hash del messaggio da rimandare
					Message toSend = sentAcks.get(mess.id);
					sendMulticast(toSend);
					
					System.out.println("Ricevuta richiesta di ritrasmissione del messaggio: ");toSend.print();
				}
				else {
					// imposto il lamport clock
					server.setLamportClock(Math.max(mess.lamport_clock, server.queryLamportClock()));
				}
				
				
				if (!mess.type.equals("unlock")) {
					// estraggo il mittente del messaggio
					sender = mess.sender;
					
					// se il mittente non è conosciuto, lo inizializzo. Imposto il suo ultimo local clock a 1, così facendo mi aspetto che
					// il prossimo sia il suo primo messaggio con local clock 1, il primo
					if (knownServers.containsKey(sender) == false) {
						knownServers.put(sender, mess.local_clock);
						expectedClock = mess.local_clock;
					}
					else if(knownServers.containsKey(sender) == true && knownServers.get(sender) > 1 && mess.local_clock == 1) {
						// in questo caso un server era attivo, aveva un local clock positivo e poi è morto. Quando torna attivo
						// il suo local clock sarà 1. Se leggo uno capisco che è tornato attivo e resetto il suo contatore
						knownServers.put(sender, mess.local_clock);
						expectedClock = mess.local_clock;
					}
					else {
						expectedClock = knownServers.get(sender);
					}
					
					// marco il messaggio come valido o non valido
					if (mess.local_clock == expectedClock) {
						mess.valid = true;
						
						// sovrascrivo il clock che mi aspetto successivamente da quel client
						knownServers.put(sender, expectedClock + 1);
					}
					else
						mess.valid = false;
					
					// ora controllo se è un ack o un messaggio
					if(mess.isAck == false) {
						
						System.out.println("--- Ricevuto messaggio ---");
						
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
						ackMsg.lamport_clock = server.getLamportClock();
						ackMsg.local_clock = server.getLocalClock();
						
						// aggingo il messaggio alla hashtable così da poterlo re-inviare in caso di retransmit
						sentAcks.put(ackMsg.getLightVersionHash(), ackMsg);
						
						// invio il mio messaggio di ack
						if (!mess.type.equals("read")) {
							sendMulticast(ackMsg);
						}
						
						System.out.println("--- Inviato multicast ---");
					}
					else {
						
						/*
						 * 
						 * controllare: se arriva l'ack ma il messaggio relativo non c'è, non lo considero
						 * 
						 */
						
						// se non è già presente lo aggiungo
						if (!ack.isAlreadyPresent(mess)) {
							
							// marco il messaggio come valido o non valido
							if (mess.local_clock == expectedClock)
								mess.valid = true;
							else
								mess.valid = false;
							
							// aggiungo l'ack alla lista delgi ack ricevuti
							ack.add(mess);
							
							System.out.println("--- Ricevuto ack ---");
						}
					}
					
					queue.print();
					ack.print();
					
					// separatore iterazioni
					System.out.println("\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n");
					
					if (isFullyAcknowledged(queue.getFirst(), ack)) {
						Message toExecute = queue.removeFirst();
						ack.removeExecuted(toExecute);
						storage.put(toExecute.id, toExecute.value);
						
						
						
						System.out.println("Messaggio eseguito: " + storage.toString());
					}
					
					/*
					 * 
					 * controllare come rendere validi gli ack dopo che arriva il messaggio precedente, ossia quello che li rende validi
					 * non erano validi perchè mancava un messaggio
					 * 
					 */
					
					
					// separatore iterazioni
					System.out.println("\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n");
				}
				else {
					// ho ricevuto un messaggio di unlock
					
					// incremento il contatore dei cicli di permanenza in coda
					queue.tick();
					
					// controllo se c'è da ritrasmettere qualcosa e la ritrasmetto
					int request = queue.checkRetransmit();
					
					if (request != -1) {
						// se c'è effettivamente un messaggio in stallo, chiedo la ritrasmissione degli ack
						Message mex = new Message("retransmit", request);
						sendMulticast(mex);
					}
				}
				
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
