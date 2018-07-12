import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class Ticker extends Thread {
	
	private int WINDOW = 2;
	private int SERVERS_PORT;
	
	private InetAddress group;
	private MulticastSocket multicast;
	public String IP;
	
	public Ticker(int port) throws IOException {
		SERVERS_PORT = port;
		
		group = InetAddress.getByName("224.0.5.1");
		multicast = new MulticastSocket(SERVERS_PORT);
		multicast.joinGroup(group);		
		IP = InetAddress.getLocalHost().getHostAddress();
	}
	
	public void sendMulticast(Message msg) throws IOException, InterruptedException {
		
		msg.source = IP;
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream(6400);
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(msg);
		byte[] data = baos.toByteArray();
		
		DatagramPacket packet = new DatagramPacket(data, data.length, group, SERVERS_PORT);
		
		multicast.send(packet);
	}
	
	public void run() {
		while(true) {
			try {
				sendMulticast(new Message("unlock"));
				Thread.sleep(WINDOW * 1000);
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
