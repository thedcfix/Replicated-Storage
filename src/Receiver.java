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
	
	private List<Message> queue;
	private List<Message> ackList;
	private Hashtable<Integer, Integer> storage;
	private List<Message> receivedMessages;
	
	public Set<String> servers;
	public SharedContent valid;
	public SharedContent db;
	
	public int DELIVERY_PORT = 8503;

	public Receiver(int port, Hashtable<Integer, ObjectOutputStream> users, Set<String> otherServers) throws IOException {
		
		SERVERS_PORT = port;
		usersTable = users;
		servers = otherServers;
		
		group = InetAddress.getByName("224.0.5.1");
		multicast = new MulticastSocket(SERVERS_PORT);
		multicast.joinGroup(group);		
		IP = InetAddress.getLocalHost().getHostAddress();
		queue = new ArrayList<>();
		ackList = new ArrayList<>();
		storage = new Hashtable<>();
		receivedMessages = new ArrayList<>();
		valid = new SharedContent(false);
		db = new SharedContent(storage);
		
		//servers = new HashSet<>();
		//servers.add("192.168.1.176");
		//servers.add("192.168.1.221");
		
		new AlivenessSender().start();
		new AlivenessChecker(SERVERS_PORT, servers, valid).start();
		new StorageUpdater(db).start();;
	}
	
	public void sendUDP(Message msg, int port) throws IOException, InterruptedException {
		
		DatagramSocket socket = new DatagramSocket();
		ByteArrayOutputStream baos = new ByteArrayOutputStream(6400);
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		
		oos.writeObject(msg);
		byte[] data_r = baos.toByteArray();
		InetAddress addr = InetAddress.getByName("localhost");
		DatagramPacket packet = new DatagramPacket(data_r, data_r.length, addr, port);
		
		socket.send(packet);
		Thread.sleep(500);
		socket.close();
	}
	
	public void sendUDPtoServer(Message msg, int port, String IP) throws IOException, InterruptedException {
		
		DatagramSocket socket = new DatagramSocket();
		ByteArrayOutputStream baos = new ByteArrayOutputStream(6400);
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		
		oos.writeObject(msg);
		byte[] data_r = baos.toByteArray();
		InetAddress addr = InetAddress.getByName(IP);
		DatagramPacket packet = new DatagramPacket(data_r, data_r.length, addr, port);
		
		socket.send(packet);
		Thread.sleep(500);
		socket.close();
	}
	
	public void sendMulticast(Message msg, boolean isAck) throws IOException, InterruptedException {
		
		if (isAck) {
			msg.isAck = true;
			msg.ackSource = IP;
		}
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream(6400);
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(msg);
		byte[] data = baos.toByteArray();
		
		DatagramPacket packet = new DatagramPacket(data, data.length, group, SERVERS_PORT);
		
		multicast.send(packet);
		Thread.sleep(500);
	}
	
	public Message receiveMessage(byte[] buff) throws IOException, ClassNotFoundException {
		DatagramPacket recv = new DatagramPacket(buff, buff.length);
		multicast.receive(recv);
		
		ByteArrayInputStream bais = new ByteArrayInputStream(buff);
		ObjectInputStream ois = new ObjectInputStream(bais);
		
		Message mess = (Message) ois.readObject();
		
		return mess;
	}
	
	private List<Message> extractAckSublist() {
		
		int finalIdx = 0;
		
		for(Message m : ackList) {
			if (m.equalsLite(queue.get(0))) {
				finalIdx++;
			}
			else {
				break;
			}
		}
		
		return ackList.subList(0, finalIdx);
		
	}
	
	private List<Message> extractOkSublist() {
		
		int finalIdx = 0;
		
		if (queue.size() != 0) {
		
			for(Message m : receivedMessages) {
				if (m.equalsLite(queue.get(0))) {
					finalIdx++;
				}
				else {
					break;
				}
			}
		
		}
		
		return receivedMessages.subList(0, finalIdx);
		
	}
	
	private List<Message> extractOkSublist(Message msg) {
		
		int finalIdx = 0;
		
		for(Message m : receivedMessages) {
			if (m.equalsLite(msg)) {
				finalIdx++;
			}
			else {
				break;
			}
		}
		
		return receivedMessages.subList(0, finalIdx);
		
	}
	
	private boolean isFullyAcknowledged() {
		
		List<Message> acks = extractAckSublist();
		
		if (acks.size() == servers.size()) {
			return true;
		}
		else {
			return false;
		}
	}
	
	private boolean isFullyOk() {
		
		List<Message> ok = extractOkSublist();
		
		if (ok.size() == servers.size()) {
			return true;
		}
		else {
			return false;
		}
	}
	
	private void manageRetransmissions(List<Message> acks) throws IOException, InterruptedException {
		
		@SuppressWarnings("unchecked")
		HashSet<String> IPs = (HashSet<String>) ((HashSet<String>) servers).clone();
		
		for (Message m : acks) {
			if (IPs.contains(m.ackSource)) {
				// dopo aver creato una copia degli IP dei server noti, rimuovo gli IP che hanno già fornito l'ack
				IPs.remove(m.ackSource);
			}
		}
		
		//agli IP rimanenti invio la richiesta di ritrasmissione
		Message msg = new Message(queue.get(0));
		
		msg.type = "ack";
		msg.isAck = true;
		msg.isRetransmit = true;
		msg.executable = false;
		msg.ackSource = this.IP;
		
		for (String IP : IPs) {
			sendUDPtoServer(msg, SERVERS_PORT, IP);
		}
	}
	
	private boolean isAlreadyPresent(Message msg) {
		boolean flag = false;
		
		for(Message m : queue) {
			if (msg.equals(m)) {
				flag = true;
				break;
			}
		}
		
		return flag;
	}
	
	private boolean isAlreadyPresent(Message msg, String mode) {
		boolean flag = false;
		
		if (mode.equals("list")) {
			for(Message m : ackList) {
				if (msg.equals(m, mode)) {
					flag = true;
					break;
				}
			}
		}
		else if (mode.equals("ok")) {
			for(Message m : receivedMessages) {
				if (msg.equals(m, mode)) {
					flag = true;
					break;
				}
			}
		}
		
		return flag;
	}
	
	private void printQueue() {
		
		System.out.println("\nCoda dei messaggi: \n");
		
		for (Message m : queue) {
			m.print();
		}
		
		System.out.println("\n");
	}
	
	private void printOk() {
		
		System.out.println("Coda dei messaggi di ok: \n");
		
		for (Message m : receivedMessages) {
			m.print();
		}
		
		System.out.println("\n");
	}
	
	private void printList() {
		
		System.out.println("Lista degli ack: \n");
		
		for(Message m : ackList) {
			m.print();
		}
		
		System.out.println("\n");
	}
	
	private void cleanQueue(List<Message> executionList) {
		try {
			
			if (queue.size() != 0) {
				for (Message m1 : queue) {
					for (Message m2 : executionList) {
						if (m1.equalsLite(m2)) {
							queue.remove(m1);
						}
					}
				}
			}
		}
		catch (Exception e) {
			;
		}
	}
	
	private void cleanAck(List<Message> executionList) {
		try {
			
			if (ackList.size() != 0) {
				for (Message m1 : ackList) {
					for (Message m2 : executionList) {
						if (m1.equalsLite(m2)) {
							ackList.remove(m1);
						}
					}
				}
			}
		}
		catch (Exception e) {
			;
		}
	}
	
	private void cleanOk(List<Message> executionList) {
		try {
			
			if (receivedMessages.size() != 0) {
				for (Message m1 : receivedMessages) {
					for (Message m2 : executionList) {
						if (m1.equalsLite(m2)) {
							receivedMessages.remove(m1);
						}
					}
				}
			}
		}
		catch (Exception e) {
			;
		}
	}
	
	private void printExecuted(List<Message> list) {
		int i = 1;
		
		System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		
		for (Message m : list) {
			System.out.print(i + " ");
			m.print();
			i++;
		}
		
		System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
	}
	
	private int count(Message msg, List<Message> list) {
		
		int counter = 0;
		
		for (Message m : list) {
			if (m.equalsLite(msg)){
				counter++;
			}
		}
		
		return counter;
	}
	
	private List<Message> extractAckList(Message msg) {
		
		List<Message> list = new ArrayList<>();
		
		for (Message m : ackList) {
			if (m.equalsLite(msg)){
				list.add(new Message(m));
			}
		}
		
		return list;
	}
	
	private boolean find(Message msg) {
		for (Message m : queue) {
			if (m.equalsLite(msg)) {
				return true;
			}
		}
		
		return false;
	}
	
	private void handleRetransmissions(long cycle, int max) throws IOException, InterruptedException {
		if(queue.size() != 0) {
			// ho un elemento in coda ma mancano ack o mesaggi di ok
			
			for (Message m : queue) {
				if(cycle >= m.cycle + max) {
					if (count(m, ackList) != servers.size()) {
						// mancano dei messaggi di ack quindi ritrasmetto tutto						
						sendMulticast(m, false);
						
						System.out.println("E' stata richiesta la ritrasmissione di un messaggio di ok relativo a:");m.print();
					}
					
					// aggiorno il valore del cycle
					m.cycle = cycle;
				}
			}
		}
		
		// potrebbe invece mancare proprio il messaggio (quello di ok ce l'ho per forza perchè altrimenti gli altri server non possono andare avanti)
		// come minimo mi arriverebbe una ritrasmissione dei loro ok
		for (Message ack : ackList) {
			if(!find(ack)) {
				// significa che il messaggio esiste ma io non lo ho, ne chiedo ritrasmissione segnandolo come missing
				Message request = new Message(ack);
				
				request.type = "missing";
				request.isAck = false;
				request.requestedAck = false;
				request.isRetransmit = true;
				request.executable = false;
				request.ackSource = IP;
				
				sendUDPtoServer(request, SERVERS_PORT, ack.ackSource);
				
				System.out.println("E' stata richiesta la ritrasmissione di un messaggio mancante a cui corrisponde il messaggio di ok:");ack.print();
			}
		}
	}
	
	public boolean checkExistance(Message msg, List<Message> executionList) {
		
		boolean present = false;
		
		// cerco in coda
		for (Message m : queue) {
			if(m.equalsPrevious(msg)) {
				present = true;
				break;
			}
		}
		
		// cerco nella lista degli eseguiti
		if (!present) {
			for (Message m : executionList) {
				if(m.equalsPrevious(msg)) {
					present = true;
					break;
				}
			}
		}
		
		return present;
	}
	
	public boolean checkAlreadyProcessed(Message msg, List<Message> executionList) {
		boolean present = false;
		
		// cerco in coda
		for (Message m : queue) {
			if(m.equals(msg)) {
				present = true;
				break;
			}
		}
		
		// cerco nella lista degli eseguiti
		if (!present) {
			for (Message m : executionList) {
				if(m.equals(msg)) {
					present = true;
					break;
				}
			}
		}
		
		return present;
	}
	
	public Message retrieveMissing(Message msg, List<Message> executionList) {
		
		// cerco in coda
		for (Message m : queue) {
			if(m.equalsLite(msg)) {
				return m;
			}
		}
		
		// cerco nella lista degli eseguiti
		for (Message m : executionList) {
			if(m.equalsLite(msg)) {
				return m;
			}
		}
		
		// non succede mai
		return null;
	}
	
	private void manageLock(List<Message> executionList) {
		if(queue.size() != 0) {
			
			Message first = queue.get(0);
			
			if (!first.hasPrevious) {
				if (count(first, ackList) == servers.size()) {
					first.executable = true;
				}
			}
			else if (!first.executable && checkExistance(first, executionList)) {
				if (count(first, ackList) == servers.size()) {
					first.executable = true;
				}
			}
			
			if (first.hasPrevious && executionList.size() == 0) {
				// significa che il server si è appena aggiunto al pool
				first.executable = true;
			}
		}
	}
	
	public void run() {
		
		byte[] buff = new byte[8192];
		long cycle = 0;
		int cyclesToRetransmit = 8;
		boolean busy = false;
		
		// lista contenente tutti i messaggi eseguiti
		List<Message> executionList = new ArrayList<>();
		
		while(true) {
			
			try {
				// ricevo il messaggio
				Message mess = receiveMessage(buff);
				
				if (!mess.type.equals("unlock") && !mess.type.equals("missing")) {
					
					// gestione messaggi di azione e ack
					if (!mess.isAck) {
						// il messaggio non è un ack
						
						if (checkAlreadyProcessed(mess, executionList) && mess.source.equals(IP)) {
							// se il messaggio esiste già significa che è stata richiesta une ritrasmissione perchè l'handler non ha ricevuto l'ack di inserimento i coda
							// quindi reinvio semplicemente quell'ack
							
							// invio conferma ricezione da parte del server all'handler
							sendUDP(mess, DELIVERY_PORT);
						}
						else {
							if (!isAlreadyPresent(mess) && mess.source.equals(IP)) {
								// il messaggio proviene dal client, che lo ha sottomesso al server
								
								// invio conferma ricezione da parte del server all'handler
								sendUDP(mess, DELIVERY_PORT);
								mess.cycle = cycle;
								
								queue.add(mess);
								
								// ordino la coda
								Collections.sort(queue, (m1, m2) -> m1.source.hashCode() - m2.source.hashCode());
								Collections.sort(queue, (m1, m2) -> m1.clock - m2.clock);
								
								// invio il mio messaggio agli altri server
								Message msg = new Message(mess);
	
								msg.isRetransmit = false;
								msg.executable = false;
								
								sendMulticast(msg, false);
								System.out.println("Messaggio inviato dal client ricevuto e inserito in coda. Inoltrati i messaggi agli altri server");mess.print();
							}
							else {
								// il messaggio proviene da un altro server
								
								if(!isAlreadyPresent(mess)) {
									// il messaggio è stato inviato da un altro server e io devo inserirlo in coda e mandare un messaggio di ok al mittente
									mess.cycle = cycle;
									
									queue.add(mess);
									
									// ordino la coda
									Collections.sort(queue, (m1, m2) -> m1.source.hashCode() - m2.source.hashCode());
									Collections.sort(queue, (m1, m2) -> m1.clock - m2.clock);
									
									System.out.println("Ricevuto messaggio proveniente da " + mess.source + ". Inserimento in coda avvenuto.");mess.print();
								}
							}
							
							// in entrambi i casi rispondo emettendo il mio messaggio di ok
							Message ackMsg = new Message(mess);
							
							ackMsg.type = "ack";
							ackMsg.isAck = true;
							ackMsg.ackSource = IP;
							ackMsg.isRetransmit = false;
							ackMsg.executable = false;
							
							// invio il mio messaggio di ok
							sendMulticast(ackMsg, false);
							
							if(!mess.source.equals(IP)) {
								System.out.println("Messaggio di ack mandato a " + mess.source + " da " + IP);
							}
						}
					}
					else {
						if (!isAlreadyPresent(mess, "list")) {
								// aggiungo l'ack alla lista delgi ack ricevuti
								Message m = new Message(mess);
								
								m.isAck = true;
								m.isRetransmit = false;
								m.executable = false;
								
								ackList.add(m);
								
								// ordino la lista
								Collections.sort(ackList, (m1, m2) -> m1.source.hashCode() - m2.source.hashCode());
								Collections.sort(ackList, (m1, m2) -> m1.clock - m2.clock);
								
								System.out.println("Ack da " + mess.ackSource + " ricevuto e inserito in lista");mess.print();
						}
					}
				}
				else {
					// gestione messaggi di missing e unlock
					if(mess.type.equals("missing")) {
							
							String destination = mess.ackSource;
							
							Message missing = new Message(retrieveMissing(mess, executionList));
							
							missing.isAck = false;
							missing.isRetransmit = false;
							missing.executable = false;
							
							sendUDPtoServer(missing, SERVERS_PORT, destination);
							System.out.println("Inviato messaggio mancante a " + destination);missing.print();
					}
					else {
						// gestione messaggi di unlock
						
						cycle++;
						
						// gestisco i messaggi bloccati in coda chiedendo eventuali ritrasmissioni
						cleanQueue(executionList);
						cleanAck(executionList);
						cleanOk(executionList);
						handleRetransmissions(cycle, cyclesToRetransmit);
					}
				}
				
				// mi assicuro che eventuali messaggi ricevuti con estremo ritardo non vadano a sporcare le code di esecuzione
				cleanQueue(executionList);
				cleanAck(executionList);
				
				// se vengono persi più messaggi in sequenza ci si ritrovacon il primo messaggio in coda non eseguibile
				// in tal caso sblocco la situazione
				manageLock(executionList);
				
				if (queue.size() != 0 && !mess.type.equals("unlock")) {
					System.out.println("\n-------------------------------------------------------------------------------------------------------\n");
					printQueue();
					System.out.println("\n-------------------------------------------------------------------------------------------------------\n");
					printList();
					System.out.println("\n-------------------------------------------------------------------------------------------------------\n");
					/*printOk();
					System.out.println("\n-------------------------------------------------------------------------------------------------------\n");*/
					System.out.println("\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n");
				}
				
				if (isFullyAcknowledged()) {
					
					while (!valid.getValidity()) {
						// aspetto la finestra di validità
						Thread.sleep(200);
					}
					
					storage = db.getStorage();
					
					while (isFullyAcknowledged() && queue.get(0).executable && valid.getValidity()) {
						Message inExecution = queue.get(0);
						executionList.add(inExecution);
						
						// cancello gli ok di conferma dei server dato che il messaggio è arrivato allo stadio finale
						//receivedMessages.removeAll(extractOkSublist(inExecution));
						
						if (inExecution.type.equals("write")) {
							storage.put(inExecution.id, inExecution.value);
						}
						else if (inExecution.type.equals("read") && inExecution.source.equals(IP)) {
							// alle read risponde solo il server a cui è connesso il client
							Integer value = storage.get(inExecution.id);
							usersTable.get(inExecution.clientID).writeObject(new Message("response", inExecution.id, value));
						}
						
						// rimuovo il messaggio eseguito dalla coda, cancello i suoi ack e lo aggiungo alla lista dei messaggi eseguiti
						ackList.removeAll(extractAckSublist());
						queue = queue.subList(1, queue.size());
						
						System.out.println("Messaggio eseguito");
						System.out.println(storage.toString());
						System.out.println("\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n");
						
						busy = false;
					}
					
					printExecuted(executionList);
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
