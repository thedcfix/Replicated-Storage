import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

public class Unchoker extends Thread {
	
	private SharedContent queue;
	public int DELIVERY_PORT;
	int chockingTime = 10;
	
	public Unchoker(SharedContent queue, int port) {
		this.queue = queue;
		DELIVERY_PORT = port;
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
	
	public void run() {
		
		Message lastFirst = null;
		long actual;
		long lastTimestamp = 0;
		
		while (true) {
			try {
				// c'è un nuovo primo elemento
				if (queue.getQueue().get(0) != null && lastFirst == null) {
					lastTimestamp = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
					actual = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
					lastFirst = queue.getQueue().get(0);
				}
				else {		
					// c'è un nuovo last first
					if (lastFirst.equals(queue.getQueue().get(0)) == false) {
						lastTimestamp = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
						actual = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
						lastFirst = queue.getQueue().get(0);
					}
					
					actual = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
					
					// rimando il messaggio se questo è bloccato nella coda e non è ancora stato consegnato al server dopo x tempo
					if (actual - lastTimestamp >= chockingTime) {
						send(queue.getQueue().get(0), DELIVERY_PORT);
					}
				}
			}
			catch (Exception e) {
				;
			}
		}
		
		
	}

}
