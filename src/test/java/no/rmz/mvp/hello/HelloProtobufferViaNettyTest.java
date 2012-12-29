package no.rmz.mvp.hello;

import static com.google.common.base.Preconditions.checkNotNull;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import no.rmz.mvp.api.proto.Rpc;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import static org.jboss.netty.channel.Channels.pipeline;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * The objective of this test is to demonstrate that we can send a single
 * instance of Rpc.RpcParam (a protobuffer generated class) over a connection
 * managed by Netty. This _should_ be trivial, but I've tried a couple of times
 * and failed so I'm no longer so sure.
 */
@RunWith(MockitoJUnitRunner.class)
public final class HelloProtobufferViaNettyTest {

    private static final Logger log = Logger.getLogger(
            no.rmz.mvp.hello.HelloProtobufferViaNettyTest.class.getName());
    private final static String PARAMETER_STRING = "Hello world";
    private final static int PORT = 7171;
    private final static String HOST = "localhost";
    private final static int FIRST_MESSAGE_SIZE = 256;

    // We need an interface to receive something into a mock
    // and this is it.
    public interface Receiver {
        public void receive(final Rpc.RpcParam param);
    }
    // This is the receptacle for the message that goes
    // over the wire.
    @Mock
    Receiver receiver;
    /**
     * The message we are going to send through the wire and hopefully pick up
     * again on the other side.
     */
    private Rpc.RpcParam sampleRpcMessage;

    @Before
    public void setUp() {
        sampleRpcMessage = Rpc.RpcParam.newBuilder().setParameter(PARAMETER_STRING).build();
        setUpServer();

    }

    public final class RpcServerHandler extends SimpleChannelUpstreamHandler {


        @Override
        public void messageReceived(
                final ChannelHandlerContext ctx, final MessageEvent e) {
            final Object message = e.getMessage();

            log.info("Received message " + message);

            // Now if the pipeline works, we'll just cast the incoming
            // message to the appropriate valye and send it on to
            // the receiver.
            final Rpc.RpcParam msg = (Rpc.RpcParam) e.getMessage();
            receiver.receive(msg);

            // Since we're done now, we can just close
            e.getChannel().close();
        }

        @Override
        public void exceptionCaught(
                ChannelHandlerContext ctx, ExceptionEvent e) {
            // Close the connection when an exception is raised.
            log.log(
                    Level.WARNING,
                    "Unexpected exception from downstream.",
                    e.getCause());
            e.getChannel().close();
        }
    }

    public final class RpcClientHandler extends SimpleChannelUpstreamHandler {

        private final Rpc.RpcParam msgToSend;

        public RpcClientHandler(final Rpc.RpcParam msgToSend) {
            checkNotNull(msgToSend);
            this.msgToSend = msgToSend;
        }

        @Override
        public void channelConnected(
                final ChannelHandlerContext ctx, final ChannelStateEvent e) {
            e.getChannel().write(msgToSend);
        }

        @Override
        public void messageReceived(
                final ChannelHandlerContext ctx, final MessageEvent e) {
            fail();
        }

        @Override
        public void exceptionCaught(
                final ChannelHandlerContext ctx, final ExceptionEvent e) {
            // Close the connection when an exception is raised.
            log.log(
                    Level.WARNING,
                    "Unexpected exception from downstream.",
                    e.getCause());
            e.getChannel().close();
            fail();
        }
    }

    public void setUpServer() {

        final ServerBootstrap bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()));


        final RpcServerHandler serverHandler = new RpcServerHandler();

        final ChannelPipelineFactory serverChannelPipeline = new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                final ChannelPipeline p = pipeline();
                p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
                p.addLast("protobufDecoder",
                        new ProtobufDecoder(Rpc.RpcParam.getDefaultInstance()));
                p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
                p.addLast("protobufEncoder", new ProtobufEncoder());
                p.addLast("handler", serverHandler);
                return p;
            }
        };

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(serverChannelPipeline);

        // Bind and start to accept incoming connections.
        bootstrap.bind(new InetSocketAddress(PORT));

    }

    private void setUpClient(final Rpc.RpcParam msgToSend) {
        checkNotNull(msgToSend);

        // Configure the client.
        final ClientBootstrap clientBootstrap = new ClientBootstrap(
                new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()));

        final ChannelPipelineFactory clientChannelPipeline = new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                final ChannelPipeline p = pipeline();
                p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
                p.addLast("protobufDecoder",
                        new ProtobufDecoder(Rpc.RpcParam.getDefaultInstance()));
                p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
                p.addLast("protobufEncoder", new ProtobufEncoder());
                p.addLast("handler", new RpcClientHandler(msgToSend));
                return p;
            }
        };

        // Set up the pipeline factory.
        clientBootstrap.setPipelineFactory(clientChannelPipeline);

        // Start the connection attempt.
        final ChannelFuture future =
                clientBootstrap.connect(new InetSocketAddress(HOST, PORT));

        // Wait until the connection is closed or the connection attempt fails.
        future.getChannel().getCloseFuture().awaitUninterruptibly();

        // Shut down thread pools to exit.
        clientBootstrap.releaseExternalResources();
    }

    @Test
    public void testTransmission() {
        // Set up a client and have it send the sample message.
        setUpClient(sampleRpcMessage);
        // Then verify that it got through.  Should be simple
        // enough?
        verify(receiver).receive(sampleRpcMessage);
    }
}
