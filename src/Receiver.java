import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
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
	
	private Hashtable<Integer, Integer> storage;
	
	public Set<String> servers;
	
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
		
		//servers
		servers = new HashSet<>();
		servers.add("192.168.1.176");
		servers.add("192.168.1.221");
		
		this.queue = queue;
		this.ack = new Queue("ack");
		
		this.server = server;

		storage = new Hashtable<>();
		
		new Ticker(SERVERS_PORT).start();
		
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
		
		Hashtable<Integer, Message> messages = new Hashtable<>();
		Hashtable<Integer, Message> acks = new Hashtable<>();
		
		while(true) {
			
			try {
				// ricevo il messaggio
				Message mess = receiveMessage(buff);
				
				// *************************************************************************************************************************
				
				if (mess.isRetransmit == true) {
					// gestione ritrasmisisone. Uso il campo id per la trasmissione dell'hash del messaggio da rimandare
					// invio sia il messaggio sia l'ack
					
					// prima controllo se ho il messaggio
					boolean gotIt = messages.containsKey(mess.id);
					
					// se lo ho invio tutto altrimenti significa che ho perso il messaggio primario. In quel caso resto in attesa che
					// il mittente me lo rimandi
					if (gotIt) {
						Message toSendM = messages.get(mess.id);
						Message toSendA = acks.get(mess.id);
						sendMulticast(toSendM);
						sendMulticast(toSendA);
						
						System.out.println("Ricevuta richiesta di ritrasmissione dei messaggi: ");toSendM.print();toSendA.print();
					}
				}
				
				// *************************************************************************************************************************
				
				if (!mess.type.equals("unlock") && !mess.isRetransmit) {
					
					// aggiorno il lamport clock
					server.setLamportClock(Math.max(mess.lamport_clock, server.queryLamportClock()) + 1);
					
					// estraggo il mittente del messaggio
					sender = mess.sender;
					
					// se il mittente non è conosciuto, lo inizializzo. Imposto il suo ultimo local clock a 1, così facendo mi aspetto che
					// il prossimo sia il suo primo messaggio con local clock 1, il primo
					if (knownServers.containsKey(sender) == false) {
						knownServers.put(sender, 1);
						expectedClock = 1;
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
					
					// se è valido lo processo, altrimenti non lo considero
					if (mess.valid) {
						
						// ora controllo se è un ack o un messaggio
						if(mess.isAck == false) {
							
							System.out.println("--- Ricevuto messaggio ---");
							
							// lo aggiungo all'elenco dei messaggi ricevuti
							messages.put(mess.getLightVersionHash(), mess);
							
							// se non è già presente ed è valido lo aggiungo (poi pulisco nel caso sia già stato processato in passato)
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
							acks.put(ackMsg.getLightVersionHash(), ackMsg);
							
							// invio il mio messaggio di ack
							if (!mess.type.equals("read")) {
								sendMulticast(ackMsg);
							}
							
							System.out.println("--- Inviato multicast ---");
						}
						else {
							
							// l'ack è valido solo se c'è un messaggio a cui è riferito, altrimenti non lo considero
							boolean res = messages.containsKey(mess.getLightVersionHash());
							
							if (res == true) {
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
						}
					}
					
					
					if (queue.isEmpty() == false) {
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
						
						
						// separatore iterazioni
						System.out.println("\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n");
					}
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
						mex.source = IP;
						mex.isRetransmit = true;
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
