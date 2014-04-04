
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;


public class TCPServer {
	  private final int port;
	  private final String host;
	  private Queue<String> queryQueue;
	  private ChannelPipeline myPipeline;

	  public TCPServer(int port) {
		  this.port = port;
		  this.host = "localhost";
		  this.queryQueue = new ConcurrentLinkedQueue<String>();
	  }
	  
	  public TCPServer(String host, int port) {
		  this.port = port;
		  this.host = host;
		  this.queryQueue = new ConcurrentLinkedQueue<String>();
	  }
	  /**
	   * Takes a query off the queryList and returns it.
	   * @return String which is a query, or null if empty
	   */
	  public String getNextQuery() {
		  return queryQueue.poll();
	  }
	  /**
	   * Sends the results back to the client
	   * @param ans - the string of the answer
	   * @return true if success, false if fail
	   */
	  public boolean sendResult(String ans) {
		  try {
			  CharSequence cs = (CharSequence)ans;
			  ChannelBuffer buf = ChannelBuffers.copiedBuffer(cs,  Charset.defaultCharset());
			  myPipeline.getChannel().write(buf);
		  } catch (Exception e) {
			  System.out.println("!! Failure to send results!");
			  return false;
		  }	  
		  return true;
	  }
	  
	  public void run() {
		  ServerBootstrap bootstrap = new ServerBootstrap (
				  new NioServerSocketChannelFactory(
						  Executors.newCachedThreadPool(),
						  Executors.newCachedThreadPool()));
		  
		  bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			  public ChannelPipeline getPipeline() throws Exception {
				  myPipeline = Channels.pipeline(new TCPServerHandler(queryQueue));
				  return myPipeline;
			  }
		  });
		  
		  bootstrap.bind(new InetSocketAddress(host, port));
	  }
	  
	  public void close() {
		  myPipeline.getChannel().close();
	  } 
  }