import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramSocket;
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
	public int DELIVERY_PORT = 8503;
	
	private final Object lock = new Object();
	private int clock = 0;
	private int clientID = 0;
	
	public DatagramSocket confirmation;
	public Hashtable<Integer, ObjectOutputStream> usersTable;
	public Set<String> otherServers;
	
	public Server() throws IOException {
		connectionsReceiver = new ServerSocket(CLIENTS_PORT);
		confirmation = new DatagramSocket(DELIVERY_PORT);
		usersTable = new Hashtable<>();
		otherServers = new HashSet<>();
		
		new Receiver(SERVERS_PORT, usersTable, otherServers).start();
	}
	
	public int getClock() {
		synchronized (lock) {
			clock++;
		}
		
		return clock;
	}
	
	public int getClientID() {
		synchronized (lock) {
			clientID++;
		}
		
		return clientID;
	}
	
	public static void main(String[] args) throws IOException {
		
		Server server = new Server();
		Socket client;
		int clientID;
		ObjectOutputStream out;
		
		System.out.println("Server avviato a " + InetAddress.getLocalHost().getHostAddress() + "...");
		
		while (true) {
        	client = server.connectionsReceiver.accept();
        	clientID = server.getClientID();
        	out = new ObjectOutputStream(client.getOutputStream());
        	
        	server.usersTable.put(clientID, out);
        	
    	    new Handler(server, client, clientID, out).start();
       }
	}

}
