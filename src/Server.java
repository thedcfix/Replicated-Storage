import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

public class Server {
	
	public ServerSocket connectionsReceiver;
	
	public int SERVERS_PORT = 8501;
	public int CLIENTS_PORT = 8502;
	
	private final Object lock = new Object();
	private int lamport_clock = 0;
	private int local_clock = 0;
	private int clientID = 0;
	
	private Queue queue;
	
	public Hashtable<Integer, ObjectOutputStream> usersTable;
	public Set<String> otherServers;
	
	public Server() throws IOException {
		connectionsReceiver = new ServerSocket(CLIENTS_PORT);
		usersTable = new Hashtable<>();
		otherServers = new HashSet<>();
		this.queue = new Queue("messaggi");
		
		new Receiver(SERVERS_PORT, usersTable, otherServers, queue).start();
	}
	
	public Queue getQueue() {
		return this.queue;
	}

	
	public int getLamportClock() {
		synchronized (lock) {
			lamport_clock++;
		}
		
		return lamport_clock;
	}
	
	public int getLocalClock() {
		synchronized (lock) {
			local_clock++;
		}
		
		return local_clock;
	}
	
	public int getClientID() {
		synchronized (lock) {
			clientID++;
		}
		
		return clientID;
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
		Server server = new Server();
		Socket client_connection;
		int clientID;
		ObjectOutputStream out;
		
		System.out.println("Server avviato a " + InetAddress.getLocalHost().getHostAddress() + "...");
		
		while (true) {
			client_connection = server.connectionsReceiver.accept();
        	clientID = server.getClientID();
        	out = new ObjectOutputStream(client_connection.getOutputStream());
        	
        	server.usersTable.put(clientID, out);
        	
    	    new Handler(server, client_connection, clientID, out).start();
       }
	}
}
