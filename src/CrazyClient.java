import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

/*
 * Client interacting with the server. It can ask for a read, a write or a disconnection.
 */

public class CrazyClient {
	
	private Socket socket;
	private ObjectOutputStream out;
	private ObjectInputStream in;
	private int CLIENTS_PORT = 8502;
	
	public CrazyClient() throws UnknownHostException, IOException {
		this.socket = new Socket("localhost", CLIENTS_PORT);
		this.out = new ObjectOutputStream(socket.getOutputStream());
		this.in = new ObjectInputStream(socket.getInputStream());
	}
	
	// read the value associated to the id
	public int read(int id) throws IOException, ClassNotFoundException {
		
		out.writeObject(new Message("read", id));
		out.flush();
		
		// attendo il responso della lettura
		Message response = (Message) in.readObject();
		
		return response.value;
	}
	
	// writes a value to a given id
	public void write(int id, int value) throws ClassNotFoundException, IOException {
		
		out.writeObject(new Message("write", id, value));
		out.flush();
		
		// attendo conferma avvenuta scrittura
		//@SuppressWarnings("unused")
		//Message txt = (Message) in.readObject();
	}
	
	// tells the server it wants to disconnect (to avoid broken connection that could generate exceptions on the server)
	public void disconnect() throws IOException, ClassNotFoundException, InterruptedException {
		
		out.writeObject(new Message("quit"));
		out.flush();
		out.close();
		in.close();
	}
	
	public void close() throws IOException {
		socket.close();
	}

	public static void main(String[] args) throws UnknownHostException, IOException, ClassNotFoundException, InterruptedException {
		
		Client client = new Client();
		Scanner reader = new Scanner(System.in);
		int id;
		int val;
		
		for (int i=0; i<50; i++) {
			
			id = (int) (Math.random() * 100 % 30);
			val = (int) (Math.random() * 100 % 30);
			
			client.write(id, val);
			System.out.println("Scritto il valore " + val + " associato all'id " + id);
			
			Thread.sleep(13000);
		}
		
		client.disconnect();
		System.out.println("Uscita del client");
		
		reader.close();
		client.close();
	}

}