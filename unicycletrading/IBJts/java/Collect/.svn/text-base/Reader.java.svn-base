package Collect;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.EOFException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

public abstract class Reader extends Thread {

    private static final String EOL = "\n";
    private static final String ACKNOWLEDGE_STR = "OK";

    public abstract void received(String str);

    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;
    private String hostname;
    private int port = -1;
    private Socket socket = null;

    public void run() {
	try {
	    if (getPort() != -1) {
		dataInputStream = new DataInputStream(getSocket().getInputStream()); 
		dataOutputStream = new DataOutputStream(getSocket().getOutputStream()); 
	    }
	    while (!isInterrupted()) {
		String str = readStr();
		if (str != null) {
		    received(str);
 		    send(ACKNOWLEDGE_STR);
		}
	    }
	    dataInputStream.close();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    private Socket getSocket()  throws IOException {
	if (socket == null) {
 	    ServerSocket server_socket = new ServerSocket();
 	    server_socket.bind(new InetSocketAddress(getHostname(), getPort()));
	    // ServerSocket server_socket = new ServerSocket(getLocalPort());
	    socket = server_socket.accept();
	}
	return socket;
    }

    protected int getPort() {
	return port;
    }

    protected String getHostname() {
	return hostname;
    }

    protected void send(String str) throws IOException {
	if (dataOutputStream != null) {
  	    dataOutputStream.writeBytes(str + EOL);
	}
    }

    protected String readStr() throws IOException {

        StringBuffer buf = new StringBuffer();

        while (true) {
	    try {
		byte c = dataInputStream.readByte();
		if ((c == 0) || ((char)c == '\n')) {
		    break;
		}
		buf.append((char)c);
	    } catch (EOFException e) {
 		interrupt();
		break;
	    } catch (Exception e) {
		e.printStackTrace();
		break;
	    }
        }

        String str = buf.toString();
        return (str.length() == 0) ? null : str;
    }

    public Reader() {
	this(System.in);
    }

    public Reader(Socket socket) {
	try {
	    dataInputStream = new DataInputStream(socket.getInputStream()); 
	    dataOutputStream = new DataOutputStream(socket.getOutputStream()); 
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public Reader(String hostname, int port) {
	this.hostname = hostname;
	this.port = port;
    }

    public Reader(String hostname, int port, InputStream inputStream) {
	if (inputStream != null) {
	    dataInputStream = new DataInputStream(inputStream);
	} else {
	    this.hostname = hostname;
	    this.port = port;
	}
    }

    public Reader(InputStream inputStream) {
    }
}