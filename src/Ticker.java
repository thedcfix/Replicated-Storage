import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
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
	
	public void send(Message msg) throws IOException, InterruptedException {
		
		DatagramSocket socket = new DatagramSocket();
		ByteArrayOutputStream baos = new ByteArrayOutputStream(6400);
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		
		oos.writeObject(msg);
		byte[] data_r = baos.toByteArray();
		InetAddress addr = InetAddress.getByName("localhost");
		DatagramPacket packet = new DatagramPacket(data_r, data_r.length, addr, SERVERS_PORT);
		
		socket.send(packet);
		Thread.sleep(500);
		socket.close();
	}
	
	public void run() {
		while(true) {
			try {
				send(new Message("unlock"));
				Thread.sleep(WINDOW * 1000);
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
