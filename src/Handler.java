import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;

public class Handler extends Thread{

	@SuppressWarnings("unused")
	private Socket client;
	private ObjectInputStream in;
	private ObjectOutputStream out;
	
	private Server server;
	
	private int SERVERS_PORT;
	public String IP;
	
	private SharedContent queue;
	
	public int DELIVERY_PORT;
	public DatagramSocket confirmation;
	
	public Handler(Server server, Socket client) throws IOException {
		this.client = client;
		this.server = server;
		
		SERVERS_PORT = server.SERVERS_PORT;
		DELIVERY_PORT = server.DELIVERY_PORT;
		
		in = new ObjectInputStream(client.getInputStream());
		out = new ObjectOutputStream(client.getOutputStream());
		
		queue = new SharedContent(new ArrayList<>());
		confirmation = server.confirmation;
		
		IP = InetAddress.getLocalHost().getHostAddress();
	}
	
	public void run() {
		
		Thread unchoker = new Unchoker(queue, SERVERS_PORT);
		unchoker.start();
		
		Message msg;
		int clientID = server.getClientID();
		boolean first = true;
		
		while(true) {
			try {
				msg = (Message) in.readObject();
				
				if (msg.type.equals("quit")) {
		           	out.writeObject(new Message("ok"));
		       		out.flush();
		       		
		           	break;
	           }
	           else {
		           msg.clock = server.getClock();
		           msg.source = IP;
		           msg.clientID = clientID;
		           
		           if (!first)
		        	   msg.hasPrevious = true;
		           else {
		        	   msg.hasPrevious = false;
		        	   first = false;
		           }
		           
		           queue.getQueue().add(msg);
		           new DeliveryService(queue, msg, SERVERS_PORT, confirmation).start();
		           
		           System.out.println("\nMessaggio del client inoltrato al receiver");
	           }
			} catch (ClassNotFoundException | IOException e) {
				;
			}
		}
		
		unchoker.interrupt();
	}
	
}
