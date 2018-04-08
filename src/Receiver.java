import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
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
import java.util.stream.Collectors;

public class Receiver extends Thread {
	
	private InetAddress group;
	private int SERVERS_PORT;
	private MulticastSocket multicast;
	public String IP;
	
	private List<Message> queue;
	private List<Message> ackList;
	private Hashtable<Integer, Integer> storage;
	private List<Message> receivedMessages;
	
	private Set<String> servers;
	
	public int DELIVERY_PORT = 8503;

	public Receiver(int port) throws IOException {
		
		SERVERS_PORT = port;
		
		group = InetAddress.getByName("224.0.5.1");
		multicast = new MulticastSocket(SERVERS_PORT);
		multicast.joinGroup(group);		
		IP = InetAddress.getLocalHost().getHostAddress();
		queue = new ArrayList<>();
		ackList = new ArrayList<>();
		storage = new Hashtable<>();
		receivedMessages = new ArrayList<>();
		servers = new HashSet<>();
		servers.add("192.168.1.176");
		servers.add("192.168.1.221");
		
		new AlivenessSender().start();
		new AlivenessChecker(SERVERS_PORT).start();
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
		Message msg = queue.get(0);
		msg.isAck = true;
		msg.ackSource = this.IP;
		msg.isRetransmit = true;
		
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
	
	private boolean valid() {
		if (queue.size() != 0) {
			if (queue.get(0).source.equals(IP)) {
				return true;
			}
			else {
				return false;
			}
		}
		
		return false;
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
	
	private int count(Message msg) {
		
		int counter = 0;
		
		for (Message m : receivedMessages) {
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
	
	private Message find(Message msg) {
		for (Message m : queue) {
			if (m.equalsLite(msg)) {
				return m;
			}
		}
		
		// non succede mai
		return null;
	}
	
	private void ackForwarding() throws IOException, InterruptedException {
		
		int number;
		
		for (int i=0; i< receivedMessages.size(); ) {
			
			Message okMsg = receivedMessages.get(i);
			number = count(okMsg);
			
			if (number == servers.size()) {				
				// invio le richieste di ack una sola volta
				if (!okMsg.requestedAck) {
					okMsg.requestedAck = true;
					
					Message request = new Message(find(okMsg));
					request.type = "send";
					
					sendMulticast(request, true);
				}
			}
			
			i += number;
		}
	}
	
	private void handleRetransmissions(long cycle, int max) throws IOException, InterruptedException {
		if(queue.size() != 0) {
			for (Message m : queue) {
				if(cycle >= m.cycle + max) {
					if (count(m) == servers.size()) {
						// il messaggio ha già tutti gli ok e quindi mancano degli ack
						manageRetransmissions(extractAckList(m));
						System.out.println("E' stata richiesta la ritrasmissione di un ack relativo a:");m.print();
					}
					else {
						// mancano dei messaggi di ok quindi ritrasmetto tutto
						Message request = new Message(m);
						request.isAck = false;
						request.requestedAck = false;
						request.isRetransmit = true;
						
						sendMulticast(request, false);
						
						System.out.println("E' stata richiesta la ritrasmissione di un messaggio di ok relativo a:");m.print();
					}
					
					// aggiorno il valore del cycle
					m.cycle = cycle;
				}
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
	
	public void run() {
		
		byte[] buff = new byte[8192];
		boolean first = true;
		long cycle = 0;
		int cyclesToRetransmit = 8;
		
		// lista contenente tutti i messaggi eseguiti
		List<Message> executionList = new ArrayList<>();
		
		// per debug
		List<Integer> clockList = new ArrayList<>();
		
		while(true) {
			
			try {
				// ricevo il messaggio
				Message mess = receiveMessage(buff);
				
				//mess.print();
				
				if (!mess.type.equals("unlock") && !mess.type.equals("ok") && !mess.type.equals("send")) {
					
					//mess.print();
					
					// gestione messaggi di azione e ack
					if (!mess.isAck) {
						// il messaggio non è un ack
						
						if (!isAlreadyPresent(mess) && mess.source.equals(IP)) {
							// il messaggio proviene dal client, che lo ha sottomesso al server
							
							// invio conferma ricezione da parte del server all'handler
							sendUDP(mess, DELIVERY_PORT);
							mess.cycle = cycle;
							
							// gestisco l'ordinamento in ricezione da parte deglia trli server
							if (!first) {
								mess.hasPrevious = true;
							}
							else {
								mess.hasPrevious = false;
								first = false;
							}
							
							// lo rendo eseguibile dato che proviene da questo server ed è necessariamente già stato ricevuto in ordine
							mess.executable = true;
							
							queue.add(mess);
							
							// ordino la coda
							Collections.sort(queue, (m1, m2) -> m1.source.hashCode() - m2.source.hashCode());
							Collections.sort(queue, (m1, m2) -> m1.clock - m2.clock);
							
							// invio il mio messaggio agli altri server
							Message msg = new Message(mess);

							msg.isRetransmit = false;
							msg.executable = false;
							
							sendMulticast(msg, false);
							System.out.println("Messaggio inviato dal client ricevuto e inserito in coda. Inoltrati i messaggi agli altri server");
						}
						else {
							// il messaggio proviene da un altro server
							
							if(!isAlreadyPresent(mess)) {
								// il messaggio è stato inviato da un altro server e io devo inserirlo in coda e mandare un messaggio di ok al mittente
								mess.cycle = cycle;
								
								if (mess.hasPrevious) {
									if(checkExistance(mess, executionList)) {
										// esiste un messaggio precedente quindi posso marcare questo come eseguibile
										mess.executable = true;
									}
									else {
										// non esiste un messaggio precedente quindi lo marco come non eseguibile e attendo la ritrasmissione del precedente
										mess.executable = false;
									}
								}
								else {
									mess.executable = true;
								}
								
								// se ricevo un messaggio che è il precedente di uno bloccato, lo sblocco
								if (queue.size() != 0) {
									if (mess.equalsPrevious(queue.get(0))) {
										queue.get(0).executable = true;
									}
								}
								
								queue.add(mess);
								
								// ordino la coda
								Collections.sort(queue, (m1, m2) -> m1.source.hashCode() - m2.source.hashCode());
								Collections.sort(queue, (m1, m2) -> m1.clock - m2.clock);
								
								System.out.println("Ricevuto messaggio proveniente da " + mess.source + ". Inserimento in coda avvenuto.");
							}
						}
						
						// in entrambi i casi rispondo emettendo il mio messaggio di ok
						Message okMsg = new Message(mess);
						
						okMsg.type = "ok";
						okMsg.ackSource = IP;
						okMsg.isRetransmit = false;
						okMsg.executable = false;
						
						// invio il mio messaggio di ok
						sendMulticast(okMsg, false);
						
						if(!mess.source.equals(IP)) {
							System.out.println("Messaggio di ok mandato a " + mess.source + " da " + IP);
						}
					}
					else {
						// il messaggio è un ack
						
						if (mess.isRetransmit) {
							// eseguo la ritrasmissione del mio ack per il messaggio inviatomi
							
							// invio in multicast il mio ack
							String destination = mess.ackSource;
							
							mess.type = "ack";
							mess.isAck = true;
							mess.isRetransmit = false;
							mess.executable = false;
							mess.ackSource = IP;
							
							sendUDPtoServer(mess, SERVERS_PORT, destination);
							System.out.println("Ack ritrasmesso a " + destination);
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
								
								System.out.println("Ack da " + mess.ackSource + " ricevuto e inserito in lista");
							}
						}
					}
				}
				else {
					// gestione messaggi di ok, send e unlock
					
					if (mess.isRetransmit && mess.type.equals("ok")) {
						// gestisco la ritrasmissione di un messaggio di ok
					}
					else {
						if (mess.type.equals("ok")) {
							if(!isAlreadyPresent(mess, "ok")){
								// gestisco il fatto che gli altri server abbiano ricevuto il mio messaggio e lo abbiano aggiunto in coda
								receivedMessages.add(mess);
								
								// ordino la lista
								Collections.sort(receivedMessages, (m1, m2) -> m1.source.hashCode() - m2.source.hashCode());
								Collections.sort(receivedMessages, (m1, m2) -> m1.clock - m2.clock);
								
								System.out.println("Ricevuto messaggio di ok da parte di " + mess.ackSource);
							}
						}
						else if(mess.type.equals("send") && mess.source.equals(mess.ackSource)) {
							// gestione messaggi di send
							
							// invio in multicast il mio ack
							mess.type = "ack";
							
							sendMulticast(mess, true);
							System.out.println("Inviato ack in multicast da " + IP);
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
				}
				
				if (queue.size() != 0 && !mess.type.equals("unlock")) {
					System.out.println("\n-------------------------------------------------------------------------------------------------------\n");
					printQueue();
					System.out.println("\n-------------------------------------------------------------------------------------------------------\n");
					printList();
					System.out.println("\n-------------------------------------------------------------------------------------------------------\n");
					printOk();
					System.out.println("\n-------------------------------------------------------------------------------------------------------\n");
					System.out.println("\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n");
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
