import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Hashtable;
import java.util.Set;

/*
 * This class generates a thread that is responsible for knowing the existance of a new connecting server.
 * When a new server is on, it sends a "hello" message to inform all the alive servers. A server receiving
 * a "hello" message updates its count of the known servers and replies with an "echo" message to give confirmation.
 * Morover, when a new server connects, it receives a copy of the current database (a map of (ID, value)) to have a consistent
 * distribution of the copies of such DB.
 */

public class ServerFinder extends Thread {
	
	private int UPDATE_PORT = 8505;
	
	private InetAddress group;
	private MulticastSocket multicast;
	public String IP;
    
    public Set<String> otherServers;
    public Hashtable<Integer, Integer> storage;
    
    
    public ServerFinder(Set<String> servers, Hashtable<Integer, Integer> db) {
    	try {
    		group = InetAddress.getByName("224.0.5.1");
    		multicast = new MulticastSocket(UPDATE_PORT);
    		multicast.joinGroup(group);
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	otherServers = servers;
    	storage = db;
    }
	
    public void sendMulticast(Message msg) throws IOException, InterruptedException {
		
		msg.source = IP;
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream(6400);
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(msg);
		byte[] data = baos.toByteArray();
		
		DatagramPacket packet = new DatagramPacket(data, data.length, group, UPDATE_PORT);
		
		multicast.send(packet);
		Thread.sleep(500);
	}

	
	@SuppressWarnings("unchecked")
	public void run() {
		
		while (true) {
			try {
				// sending hello message
				
				Message hello = new Message("hello");
				hello.source = InetAddress.getLocalHost().getHostAddress();
				
				sendMulticast(hello);
				
				byte[] buffer = new byte[4096];
				
				while (true) {
					try {
						// getting responses
						DatagramPacket receivePacket = new DatagramPacket(buffer, buffer.length);
						multicast.receive(receivePacket);
						
						ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
			            ObjectInputStream ois = new ObjectInputStream(bais);
			            
			            Object communication = ois.readObject();
						
			            
			            // the message can be a Message or the DB (hashtable). The code exploits the try-cathc to handle the two cases
			            try {
			            	
							Message mess = (Message) communication;
							
							if(mess.type.equals("hello") && !mess.source.equals(InetAddress.getLocalHost().getHostAddress())) {
								
								// sending the update to keep everything consistent
								ByteArrayOutputStream baos = new ByteArrayOutputStream(6400);
								ObjectOutputStream oos = new ObjectOutputStream(baos);
								oos.writeObject(storage);
								byte[] data = baos.toByteArray();
								
								DatagramPacket hi = new DatagramPacket(data, data.length, group, UPDATE_PORT);
								
								multicast.send(hi);
							}
			            }
			            
			            catch (Exception e) {
			            	// Exploits the fact that the update message is not an instance of Message but is a HashMap. That generates
			            	// an exception (wanted) that brings here.
			            	
			            	// Updating the local copy of the data.
			            	storage = ((Hashtable<Integer, Integer>) communication);
			            	System.out.println("Aggiornato DB locale: " + storage.toString());
			            }
						
					} catch (IOException | ClassNotFoundException e) {
						e.printStackTrace();
					}
				}
				
				
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
}
