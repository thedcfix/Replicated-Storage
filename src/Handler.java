import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.Socket;
import java.util.Hashtable;

public class Handler extends Thread{

	@SuppressWarnings("unused")
	private Socket client_connection;
	private ObjectInputStream in;
	private ObjectOutputStream out;
	
	private Server server;
	private int clientID;
	
	private InetAddress group;
	private MulticastSocket multicast;
	private int SERVERS_PORT;
	public String IP;
	
	private Queue queue;
	private Hashtable<String, Integer> progressTable;
	
	public int DELIVERY_PORT;
	
	public Handler(Server server, Socket client_connection, int clientID, ObjectOutputStream out, 
			Hashtable<String, Integer> progressTable) throws IOException {
		this.client_connection = client_connection;
		this.server = server;
		this.clientID = clientID;
		
		SERVERS_PORT = server.SERVERS_PORT;
		
		in = new ObjectInputStream(client_connection.getInputStream());
		
		this.queue = server.getQueue();
		this.progressTable = progressTable;
		
		IP = InetAddress.getLocalHost().getHostAddress();
		
		group = InetAddress.getByName("224.0.5.1");
		multicast = new MulticastSocket(SERVERS_PORT);
		multicast.joinGroup(group);		
	}
	
	public void sendMulticast(Message msg) throws IOException, InterruptedException {
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream(6400);
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(msg);
		byte[] data = baos.toByteArray();
		
		DatagramPacket packet = new DatagramPacket(data, data.length, group, SERVERS_PORT);
		
		multicast.send(packet);
	}
	
	public void run() {
		
		Message msg;
		
		while(true) {
			try {
				msg = (Message) in.readObject();
				
				if (msg.type.equals("quit")) {
		           	out.writeObject(new Message("ok"));
		       		out.flush();
		       		
		           	break;
	           }
	           else {
	        	   
	        	   // assegno il lamport clock
	        	   msg.lamport_clock = server.getLamportClock();
	        	   
	        	   // assegno il local clock
	        	   msg.local_clock = server.getLocalClock();
	        	   
	        	   // assegno IP server sorgente e clientID
		           msg.source = IP;
		           msg.sender = IP;
		           msg.clientID = clientID;
		           
		           // imposto che non è un ack e che di default non è valido (non può essere eseguito dunque)
		           msg.isAck = false;
		           msg.valid = false;
		           
		           // aggiorno la progress table
		           progressTable.put(IP, msg.local_clock);
		           
		           queue.add(msg);
		           
		           sendMulticast(msg);
	           }
			} catch (ClassNotFoundException | IOException | InterruptedException e) {
				;
			}
		}
	}
	
}
