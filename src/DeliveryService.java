import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

public class DeliveryService extends Thread {
	
	private SharedContent queue;
	private Message msg;
	private int port;
	private DatagramSocket confirmation;

	public DeliveryService(SharedContent queue, Message msg, int port, DatagramSocket confirmation) throws SocketException {
		this.queue = queue;
		this.msg = msg;
		this.port = port;
		
		this.confirmation = confirmation;
	}
	
	public void send(Message msg, int port) throws IOException, InterruptedException {
		
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
	
	public boolean equals(Message m1, Message m2) {
		return m1.clientID == m2.clientID && m1.clock == m2.clock && m1.id == m2.id && /*m1.source.equals(m2.source) &&*/ m1.value == m2.value;
	}
	
	public void run() {
		
		byte[] buffer = new byte[4096];
		boolean alreadySent = false;
		
		while (true) {
			try {
				// invio il messaggio solo se è primo nella lista dei messaggi inviati dal client
				if (equals(msg, queue.getQueue().get(0)) && alreadySent == false) {
					send(msg, port);
					alreadySent = true;
					System.out.println("");
				}
				
				DatagramPacket receivePacket = new DatagramPacket(buffer, buffer.length);
	            confirmation.receive(receivePacket);
	           
	            ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
	            ObjectInputStream ois = new ObjectInputStream(bais);
				
				Message mess = (Message) ois.readObject();
				
				// significa che il messaggio è stato correttamente consegnato al receiver, quindi lo rimuovo dalla lista
				if (equals(mess, queue.getQueue().get(0))) {
					queue.setQueue(queue.getQueue().subList(1, queue.getQueue().size()));
					break;
				}
				
			} catch (IOException | ClassNotFoundException | InterruptedException e) {
				e.printStackTrace();
			}
		
		}
		
	}
}
