
import java.nio.charset.Charset;
import java.util.Queue;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

public class TCPServerHandler extends SimpleChannelUpstreamHandler {
	  private Queue<String> queryQueue;
	  
	  public TCPServerHandler(Queue<String> ql) {
		  this.queryQueue = ql;
	  }
	  
	  @Override
	  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		  ChannelBuffer buf = (ChannelBuffer)e.getMessage();
		  String q = buf.toString(Charset.defaultCharset());
		  //System.out.println(q);
		  queryQueue.add(q);
	  }

	  @Override
	  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		  e.getChannel().close();
	  }
	  
  }