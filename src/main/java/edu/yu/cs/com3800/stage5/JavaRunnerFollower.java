package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;


import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static java.lang.Thread.sleep;

public class JavaRunnerFollower implements Runnable{
    LinkedBlockingQueue<Message>sender;

    Logger logger;
    JavaRunner javaRunner=new JavaRunner();
    PeerServerImpl peerServer;
    ServerSocket serverSocket;
    public long currentJob=0;
    public feedback feedback=new feedback();


    public JavaRunnerFollower(Logger logger,PeerServerImpl peerServer) throws IOException {
        this.logger=logger;
        logger.info( "server "+peerServer.getName()+"  initializing java runner");
        this.sender=sender;

        this.peerServer=peerServer;
        try {
            serverSocket=new ServerSocket(peerServer.getUdpPort()+2);
            //serverSocket.setSoTimeout(75);
        }catch (Exception e){}
    }
    /**
     * When an object implementing interface {@code Runnable} is used
     * to create a thread, starting the thread causes the object's
     * {@code run} method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method {@code run} is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    public void shutdown(){
        try {
            if(serverSocket!=null) serverSocket.close();
        } catch (IOException e) {
            logger.warning(e.getMessage());
        }
    }

    @Override
    public void run() {
        InputStream job;
        String reluts;
        while (true){
            feedback.value=0;//reset, otherwise one bad message makes every later job get skipped
            job=reviveJob(feedback);
            if(feedback.value==1)
                return;
            if(feedback.value==2)continue;
            long jobId=currentJob;
            reluts= ExecuteJob(job);
            //keeps retrying until whoever is leader (old, or the one elected after it dies) takes the result
            deliverResult(jobId,reluts);
            if(peerServer.shutdown)return;
        }

    }

    /**
     * Results already handed to a leader. If that leader dies before forwarding them to the gateway,
     * they are re-sent to the newly elected leader (see resendToNewLeader). Bounded so it can't grow forever;
     * the gateway ignores duplicates.
     */
    private static final int RETAIN = 100;
    private final Map<Long,String> deliveredResults = new LinkedHashMap<>(){
        @Override
        protected boolean removeEldestEntry(Map.Entry<Long,String> eldest){
            return size()>RETAIN;
        }
    };

    /** TCP address of the current leader, or null if there isn't one right now */
    private InetSocketAddress leaderTcpAddress(){
        Vote leader=peerServer.getCurrentLeader();
        if(leader==null)return null;
        InetSocketAddress udp;
        if(leader.getProposedLeaderID()==peerServer.getServerId())
            udp=peerServer.getAddress();//I became leader: hand it to my own RoundRobinLeader
        else
            udp=peerServer.getPeerByID(leader.getProposedLeaderID());
        if(udp==null)return null;
        return new InetSocketAddress(udp.getHostString(),udp.getPort()+2);
    }

    private boolean trySend(long jobId,String result){
        InetSocketAddress tcp=leaderTcpAddress();
        if(tcp==null)return false;
        Message message=peerServer.sendtcpmessge(Message.MessageType.COMPLETED_WORK,result.getBytes(),tcp,jobId);
        try (Socket socketOut=new Socket(message.getReceiverHost(),message.getReceiverPort())){
            OutputStream out=socketOut.getOutputStream();
            out.write(message.getNetworkPayload());
            out.flush();
            return true;
        }catch (IOException e){
            logger.warning("could not send result of job "+jobId+" to leader at "+tcp+", will retry: "+e.getMessage());
            return false;
        }
    }

    /** blocks until a live leader accepts the result (or this server shuts down) */
    public void deliverResult(long jobId,String result){
        while (!peerServer.shutdown){
            if(peerServer.getPeerState()!= PeerServer.ServerState.LOOKING&&trySend(jobId,result)){
                synchronized (deliveredResults){
                    deliveredResults.put(jobId,result);
                }
                return;
            }
            try {
                sleep(500);
            } catch (InterruptedException e) {
                if(peerServer.shutdown)return;
            }
        }
    }

    /** called after a new leader is elected: the old leader may have died holding results it never forwarded */
    public void resendToNewLeader(){
        List<Map.Entry<Long,String>> copy;
        synchronized (deliveredResults){
            copy=new ArrayList<>(deliveredResults.entrySet());
        }
        logger.info("re-sending "+copy.size()+" completed results to new leader");
        for(Map.Entry<Long,String> e:copy)
            deliverResult(e.getKey(),e.getValue());
    }
   public String ExecuteJob(InputStream work){
        String out;
        try {
            out =javaRunner.compileAndRun(work);
        } catch (Exception e) {

            out = e.getMessage() + "\n";
        }
        return out;

   }



   public InputStream reviveJob(feedback feedback){
        Socket socket=null;
        while (socket==null&&peerServer.getPeerState()!= PeerServer.ServerState.LEADING)
        try {
            socket=serverSocket.accept();
        } catch (SocketException e) {
            //socket closed by shutdown() or because we became leader: stop instead of spinning on accept()
            if(serverSocket.isClosed()||peerServer.getPeerState()== PeerServer.ServerState.LEADING){
                feedback.value=1;
                return null;
            }
        }catch (IOException e){
            logger.warning(e.getMessage());
            throw new RuntimeException();
        }
        if(socket==null){
            feedback.value=1;
            return null;
        }
        InputStream byststreem=null;
        try {
            byststreem=socket.getInputStream();
        } catch (IOException e) {
            logger.warning(e.getMessage());
            throw new RuntimeException(e);
        }

       Message message=new Message(Util.readAllBytesFromNetwork(byststreem));

        if(message.getMessageType()!= Message.MessageType.WORK){
            feedback.value=2;
            return null;
        }
        currentJob=message.getRequestID();
       return new ByteArrayInputStream( message.getMessageContents());

   }



}
