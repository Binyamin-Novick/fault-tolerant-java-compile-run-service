package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GatewayServer extends Thread {

    private static final Logger logger =Logger.getLogger(GatewayServer.class.getName());
    HttpServer server;
    public volatile Long id=0l;
    GatewayServer gat=this;
    public GatewayPeerServerImpl gatewayPeerServer;
    public GatewayServer(int httpPort, int peerPort, long peerEpoch, Long serverID,
                         ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress, int numberOfObservers) throws IOException{
        server=HttpServer.create(new InetSocketAddress(httpPort),0);
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        server.setExecutor(executor);
        logger.log(Level.INFO,"starting up with port");
        server.createContext("/compileandrun", new Handler());
        gatewayPeerServer =new GatewayPeerServerImpl(peerPort,peerEpoch,serverID,peerIDtoAddress,serverID,numberOfObservers);

    }

    @Override
    public void run() {
        gatewayPeerServer.start();
        server.start();
    }

    class Handler implements HttpHandler{



        @Override
        public void handle(HttpExchange exchange) throws IllegalArgumentException, IOException {
            logger.log(Level.INFO,"server handling new request");
            if(!exchange.getRequestMethod().equals("POST")){
                exchange.sendResponseHeaders(405,0);
                logger.log(Level.INFO,"request is not post");
                return;

            }
            if(!exchange.getRequestHeaders().get("Content-type").get(0).equals("text/x-java-source")){
                exchange.sendResponseHeaders(400,0);
                logger.log(Level.INFO,"request is not current content type\n");
                return;
            }
            JavaRunner javaRunner =new JavaRunner();
            InputStream body=  exchange.getRequestBody();
            byte[] requestasbytes =Util.readAllBytes(body);
            String requestasString=new String(requestasbytes);
            Message tosend=null;
            try {

            while (gatewayPeerServer.currentLeader==null);;
            synchronized (gat){
                InetSocketAddress adrudp=null;
                InetSocketAddress tcp;
                while (tosend==null) {
                    if (adrudp == null)
                        adrudp = gatewayPeerServer.getPeerByID(gatewayPeerServer.currentLeader.getProposedLeaderID());
                    logger.info("gate 78");
                    tcp = new InetSocketAddress(adrudp.getAddress().getHostName(), adrudp.getPort() + 2);
                    tosend = gatewayPeerServer.sendtcpmessge(Message.MessageType.WORK, requestasbytes, tcp, id++);
                }}}catch (Exception e){
                logger.info(e.getMessage());
                return;
            }
            gatewayPeerServer.setmessge(tosend,requestasString.hashCode());



            String respons=null;
            while (respons==null){
                respons=gatewayPeerServer.getresponce(requestasString.hashCode());
            }

            logger.log(Level.INFO,"valide java seting up return of output\n");

            byte[] responseAsbytes=respons.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(200, responseAsbytes.length);

            OutputStream to = exchange.getResponseBody();
            try {

                to.write(responseAsbytes,0,responseAsbytes.length);
            } catch (IOException e) {
                e.printStackTrace(); // Print stack trace to see if there is an error
            }finally {
                to.close();
            }
            logger.log(Level.INFO,"post has been compleeted");
            //exchange.sendResponseHeaders(200, responseAsbytes.length);



        }}
}
