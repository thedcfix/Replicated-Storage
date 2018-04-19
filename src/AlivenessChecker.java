import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class AlivenessChecker extends Thread {
	
	private int WINDOW = 8;
	private int HOLDING_TIME = 3;
	private int SERVERS_PORT;
	private int SYNC_PORT = 8504;
	
	private InetAddress group;
	private MulticastSocket multicast;
	public String IP;
	
	public Set<String> servers;
	public SharedContent valid;
	
	public AlivenessChecker(int serversPort, Set<String> otherServers, SharedContent validity) throws IOException {
		group = InetAddress.getByName("224.0.5.1");
		multicast = new MulticastSocket(SYNC_PORT);
		multicast.joinGroup(group);		
		IP = InetAddress.getLocalHost().getHostAddress();
		servers = otherServers;
		valid = validity;
		
		SERVERS_PORT = serversPort;
	}
	
	public void sendMulticast(Message msg) throws IOException, InterruptedException {
		
		msg.source = IP;
		
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
	
	public void run() {
		
		byte[] buffer = new byte[4096];
		boolean flag = true;
		
		long time = 0;
		long time_finish = 0;
		
		while(true) {
			try {
				
				Message mess = receiveMessage(buffer);
				
				servers.add(mess.source);
				
				if (flag) {
					// from now on it has X seconds (time window) to collect all the participants
					time = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
					flag = false;
				}
				
				time_finish = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
				
				if(time_finish >= time + WINDOW && flag == false) {
					
					valid.setValidity(true);
					
					// mando messaggio che conferma l'acquisizione del numero totale dei server. Il numero è garantito che non cambierà
					// durante tutto l'holding time
					
					sendMulticast(new Message("unlock"));
					Thread.sleep(HOLDING_TIME * 1000);
					
					valid.setValidity(false);
					
					servers.clear();
					flag = true;
				}
				
				
			} catch (ClassNotFoundException | IOException | InterruptedException e) {
				e.printStackTrace();
			}
			
		}
	}

}
