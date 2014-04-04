import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class containerServer {

	
	static String getHostName() throws UnknownHostException {
		return InetAddress.getLocalHost().getHostName();
	}

	public static int findFreePort() throws IOException {
		ServerSocket server = new ServerSocket(0);
		int port = server.getLocalPort();
		server.close();
		return port;
	}
	
	public static String getNextMsg(TCPServer tcpServer) {
		String query1 = null;
		while (true) {
			query1 = tcpServer.getNextQuery();
			if(query1 == null) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					System.out.println("Thread interrupted from sleep?" + e.getLocalizedMessage());
				}
				continue;
			} else {
				query1 = query1.trim();
				break;
			}
		}
		return query1;
	}
	
	public static String getNextMsg(TCPClient tcpClient) {
		String reply = null;
		while(true) {
			reply = tcpClient.getNextMessage();
			if(reply == null){
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					System.out.println("Thread interrupted from sleep?" + e.getLocalizedMessage());
				}
				continue;
			} else {
				reply = reply.trim();
				break;
			}
		}
		return reply;
	}
	
	public static String executeCmdGetOp(String cmd) throws IOException, InterruptedException {
		Runtime rt = Runtime.getRuntime();
		Process proc = rt.exec(cmd);
		proc.waitFor();
	    BufferedReader reader = 
	         new BufferedReader(new InputStreamReader(proc.getInputStream()));
	    String output = "";
	    String line = "";			
	    while ((line = reader.readLine())!= null) {
	    	output = output.concat(line).concat("\n");
	    }
	    return output;
	}
	
	public static String getTimeStamp() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		return ("[" + dateFormat.format(cal.getTime()) + "]");
	}
	
	public static void logMessage(String msg) {
		System.out.println(getTimeStamp() + msg);
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(args.length != 3 ) {
			System.out.println("Provide required command line arguments !!");
			System.exit(0);
		}
		
		String clientHost = args[0];
		int clientPort = Integer.parseInt(args[1]);
		String appId = args[2];
		//String outputFile = "output_"+appId;
		String cmd1 = "./exec_query1.sh" + " " + appId;
		//String cmd2 = "2>&1 1>" + outputFile;
		
		String myHost = getHostName();
		int myPort = findFreePort();
		
		
		logMessage("amHost = " + clientHost + " amPort = "+clientPort + " appId = " + appId);
		String msg = "connect" + " " +  myHost + " " + myPort;
		logMessage("msg: " + msg);
		
		TCPServer containerServer = new TCPServer(myHost, myPort);
		containerServer.run();
		
		
		TCPClient client = new TCPClient(clientHost, clientPort);
		client.init();
		client.sendMsg(msg);
		
		
		//amClient.closeConnection();
		
		//msg = getNextMsg(amClient);
		//System.out.println(msg);
		
		
		//msg = getNextMsg(containerServer);
		//System.out.println(msg);
		
		while(true) {
			logMessage("Waiting for query from Client ..");
			String query = getNextMsg(containerServer);
			logMessage("Got Query: " + query);
			if(query.equalsIgnoreCase("exit")) {
				containerServer.close();
				logMessage("Exiting server as got exit from Client ..");
				break;
			}
			String cmd = cmd1 + " " + "\"" + query + "\"";
			logMessage("Cmd to execute:" + cmd);
			logMessage(executeCmdGetOp(cmd));
			client.closeConnection();
			client = new TCPClient(clientHost, clientPort);
			client.init();
			client.sendMsg("success");
			logMessage("Send reply to client with success ..");
		}
	}

}
