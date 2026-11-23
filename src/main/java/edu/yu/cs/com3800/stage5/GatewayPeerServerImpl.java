package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LeaderElection;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.Vote;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class GatewayPeerServerImpl extends PeerServerImpl{
    //LinkedBlockingQueue<Message> outtcp;
    LinkedBlockingQueue <Message>inttcp;
    ServerSocket serverSocket;
    GatewayPeerServerImpl gatewayServer;
    public PeerServerImpl sup;
    public GatewayPeerServerImpl(int udpPort, long peerEpoch, Long serverID, Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers) throws IOException {
        super(udpPort, peerEpoch, serverID, peerIDtoAddress, gatewayID);
        super.setPeerState(ServerState.OBSERVER);
       // this.outgoingMessages=new LinkedBlockingQueue<>();
       // outtcp=new LinkedBlockingQueue<>();
        inttcp=new LinkedBlockingQueue<>();
        serverSocket=new ServerSocket(udpPort+2);
        serverSocket.setSoTimeout(75);
        gatewayServer=this;

    }
    HashMap<Integer,Boolean> hasrecieved= new HashMap<>();
    HashMap<Integer,String> requestrespons=new HashMap<>();
    HashMap<Long,Integer> idtorequest=new HashMap<>();

    public synchronized  void setmessge(Message message,int has){
        if(hasrecieved.get(has)!=null)return;
        hasrecieved.put(has,true);
        inttcp.offer(message);
        idtorequest.put(message.getRequestID(),has);

    }
    public  String getresponce(int request){
        synchronized (this){
        return requestrespons.get(request);}
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }

    @Override
    public void run(){


        //step 2: create and run thread that listens for messages sent to this server
        logger.info("receiver starting");

        beatHart beatHart = null;
        try {
            beatHart = new beatHart(this);
            beatbox = beatHart;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        //currentLeader=leaderElection.lookForLeader();
        new leaderchecker(notGossipe);
        logger.info("gatway current leader set leader is"+currentLeader.toString());

        while (!shutdown&&!isInterrupted()){
            //if gossip says the leader died, forget it and wait for the NEW_LEADER message from the new election
            Vote leader=gatewayServer.currentLeader;
            if(leader!=null&&isPeerDead(leader.getProposedLeaderID())){
                logger.info("gateway: leader "+leader.getProposedLeaderID()+" is dead, waiting for new leader");
                gatewayServer.currentLeader=null;
                new leaderchecker(notGossipe);
                logger.info("gateway: new leader is "+gatewayServer.currentLeader);
            }
            try {
                Message out = new Message(Util.readAllBytesFromNetwork(serverSocket.accept().getInputStream()));

                synchronized (this){
                    int resp =idtorequest.get(out.getRequestID());
                    requestrespons.put(resp,new String(out.getMessageContents()));
                }

            } catch (Exception e) {

            }
            try {
                Message in=inttcp.poll(150,TimeUnit.MILLISECONDS);
                if(in!=null){
                    Vote current=gatewayServer.currentLeader;
                    if(current==null){
                        inttcp.offer(in);//no leader right now, try again later
                    }else {
                        //address the work to whoever is leader *now*, not who was leader when the request came in
                        InetSocketAddress udp=getPeerByID(current.getProposedLeaderID());
                        InetSocketAddress tcp=new InetSocketAddress(udp.getHostString(),udp.getPort()+2);
                        Message out=sendtcpmessge(Message.MessageType.WORK,in.getMessageContents(),tcp,in.getRequestID());
                        try {
                            new Socket(out.getReceiverHost(), out.getReceiverPort()).getOutputStream().write(out.getNetworkPayload());
                        }catch (IOException e){
                            logger.warning("gateway failed to send work to leader, will retry: "+e.getMessage());
                            inttcp.offer(in);
                        }
                    }
                }
            } catch (InterruptedException e) {
                if(shutdown)break;
            }

        }
        try {
            serverSocket.close();
        } catch (IOException e) {
            return;
        }
    }

    class leaderchecker{
        private leaderchecker(LinkedBlockingQueue<Message> incoming){
            while (gatewayServer.currentLeader==null){
                try {
                    Message message =incoming.poll(35, TimeUnit.MILLISECONDS);
                    if(message!=null)
                    if(message.getMessageType()== Message.MessageType.NEW_LEADER_GETTING_LAST_WORK){
                        Vote v=LeaderElection.getNotificationFromMessage(message);
                        //old NEW_LEADER messages from the previous election are still queued; skip ones naming a dead leader
                        if(!isPeerDead(v.getProposedLeaderID()))
                            gatewayServer.setCurrentLeader(v);
                    }

                }catch (Exception e){
                    logger.info(e.getMessage());
                }
            }
        }
    }

}
