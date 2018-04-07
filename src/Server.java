import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
	
	public ServerSocket connectionsReceiver;
	
	public int SERVERS_PORT = 8501;
	public int CLIENTS_PORT = 8502;
	public int DELIVERY_PORT = 8503;
	
	private final Object lock = new Object();
	private int clock = 0;
	private int clientID = 0;
	
	public DatagramSocket confirmation;
	
	public Server() throws IOException {
		connectionsReceiver = new ServerSocket(CLIENTS_PORT);
		confirmation = new DatagramSocket(DELIVERY_PORT);
		
		new Receiver(SERVERS_PORT).start();
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
		
		System.out.println("Server avviato a " + InetAddress.getLocalHost().getHostAddress() + "...");
		
		while (true) {
        	client = server.connectionsReceiver.accept();
    	    new Handler(server, client).start();
       }
	}

}
