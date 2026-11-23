package edu.yu.cs.com3800.stage5;


import edu.yu.cs.com3800.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;


public class PeerServerImpl extends Thread implements PeerServer , LoggingServer{
    private final InetSocketAddress myAddress;
    private final int myPort;
    private volatile ServerState state;
    public volatile boolean shutdown;
 //   public volatile boolean recive=false;
    public LinkedBlockingQueue<Message> outgoingMessages;
    public LinkedBlockingQueue<Message> notGossipe;
    public LinkedBlockingQueue<Message>Gossipe;
    private Long id;
    public long peerEpoch;
    protected volatile Vote currentLeader;
    private Map<Long,InetSocketAddress> peerIDtoAddress;

    public UDPMessageSender senderWorker;
    public UDPMessageReceiver receiverWorker;
    int quorum;
    LeaderElection leaderElection;
    Logger logger=Logger.getAnonymousLogger();
    public Long gatway;
    public beatHart beatbox;
    public volatile RoundRobinLeader roundRobinLeader;
    volatile JavaRunnerFollower follower;
    public ConcurrentHashMap<Long,Boolean>isalive=new ConcurrentHashMap<>();
    public PeerServerImpl(int udpPort, long peerEpoch, Long serverID, Map<Long, InetSocketAddress>
            peerIDtoAddress, Long gatewayID) throws IOException{

        logger=initializeLogging("serverimp"+serverID);
        gatway=gatewayID;
        //code here...
        this.myPort=udpPort;
        this.peerEpoch=peerEpoch;
        this.id=serverID;
        this.peerIDtoAddress=peerIDtoAddress;
        this.outgoingMessages=new LinkedBlockingQueue<>();
        this.notGossipe =new LinkedBlockingQueue<>();
        this.Gossipe=new LinkedBlockingQueue<>();
        this.myAddress=new InetSocketAddress("localhost",myPort);
        senderWorker=new UDPMessageSender(outgoingMessages,myPort);
        senderWorker.start();
        receiverWorker= new UDPMessageReceiver(notGossipe,Gossipe,myAddress,myPort,this);
        receiverWorker.start();
        state=ServerState.LOOKING;
        quorum=(peerIDtoAddress.size())/2;
        logger.info("server " + id +"starting up on prot "+myPort + "with pearEpoch of"+peerEpoch);
        //logger.setLevel(Level.OFF);




    }
    public boolean isPeerDead(long peerID){
        if(isalive.get(peerID)!=null)return true;
        return false;
    }
    public boolean isPeerDead(InetSocketAddress address){

        Long i=null;
        for (Map.Entry<Long,InetSocketAddress> e:peerIDtoAddress.entrySet()){
            InetSocketAddress peer=e.getValue();
            if(peer.getPort()==address.getPort()&&peer.getHostString().equals(address.getHostString())){
                i=e.getKey();
                break;
            }
        }
        if(i==null){
            return false;
        }
        return isPeerDead(i);
    }

    @Override
    public void shutdown(){
        logger.info("shutdown has been set true");
        this.shutdown = true;
        //actually stop everything this server owns, otherwise a "dead" server keeps answering on its ports
        if(beatbox!=null) beatbox.timer.cancel();
        senderWorker.shutdown();
        receiverWorker.shutdown();
        if(roundRobinLeader!=null) roundRobinLeader.shutdown();
        if(follower!=null) follower.shutdown();
        this.interrupt();


    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        logger.info("setting curnt leader to server"+getServerId()+"with epoch of "+v.getPeerEpoch());
        this.currentLeader=v;
    }

    @Override
    public Vote getCurrentLeader() {
        return currentLeader;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        //logger.info("sending message to socket"+target.toString());
        outgoingMessages.offer(new Message(type,messageContents,myAddress.getHostName(),myPort,target.getHostName(),target.getPort()));
        if(state==ServerState.OBSERVER){
            int g=0;
        }
    }
    public Message sendtcpmessge (Message.MessageType type, byte[] messageContents, InetSocketAddress target,long id) throws IllegalArgumentException {

        return new Message(type,messageContents,myAddress.getHostName(),myPort+2,target.getHostName(),target.getPort(),id);
    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        logger.info("server"+id+"broadcasting"+getPeerState()+"shut"+shutdown);
        for (InetSocketAddress address:peerIDtoAddress.values())
            sendMessage(type,messageContents,address);
    }

    @Override
    public ServerState getPeerState() {
        return state;
    }

    @Override
    public void setPeerState(ServerState newState) {
        logger.info("setting stat to"+ newState.getChar());
        state=newState;
    }

    @Override
    public Long getServerId() {
        return id;
    }

    @Override
    public long getPeerEpoch() {
        return peerEpoch;
    }

    @Override
    public InetSocketAddress getAddress() {
        return myAddress;
    }

    @Override
    public int getUdpPort() {
        return myPort;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return peerIDtoAddress.get(peerId);
    }

    @Override
    public int getQuorumSize() {
        return quorum;
    }
    public void elect(){
        this.state=ServerState.LOOKING;

    }

    @Override
    public void run(){
        try {
            beatbox=new beatHart(this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
           currentLeader= new LeaderElection(this,notGossipe,logger).lookForLeader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if(state==ServerState.LEADING){
            startLeader();
        }
        else {
            try {
                follower=new JavaRunnerFollower(logger,this);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Thread followerThread=new Thread(follower);
            followerThread.setDaemon(true);
            followerThread.start();
        }
        while(!shutdown){
            try {
                Thread.sleep(200);//don't busy spin
            } catch (InterruptedException e) {
                if(shutdown)break;
            }

            switch (state){
                case FOLLOWING: {
                    Vote leader=currentLeader;
                    if(leader!=null&&isPeerDead(leader.getProposedLeaderID())) {
                        logger.info("leader "+leader.getProposedLeaderID()+" is dead, starting new election");
                        state=ServerState.LOOKING;
                        //null the leader so the receiver doesn't throw away election messages as "stray"
                        currentLeader=null;
                        try {
                            currentLeader = new LeaderElection(this, notGossipe, logger).lookForLeader();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        JavaRunnerFollower myFollower=follower;
                        if(state==ServerState.LEADING){
                            //release the follower's TCP port so the leader can bind it
                            if(myFollower!=null){
                                myFollower.shutdown();
                                follower=null;
                            }
                            startLeader();
                        }
                        //the dead leader may have been holding results it never forwarded to the gateway:
                        //re-send them to the new leader (which is me, if I just became leader), who forwards them on
                        if(myFollower!=null){
                            Thread resend=new Thread(myFollower::resendToNewLeader,"resend-results-"+id);
                            resend.setDaemon(true);
                            resend.start();
                        }
                    }
                    break;
                }
                case LEADING: {
                    List<Long> gone=new ArrayList<>(beatbox.dead.keySet());
                    beatbox.dead.clear();
                    if(!gone.isEmpty()&&roundRobinLeader!=null) {
                        try {
                            roundRobinLeader.workerfail(gone);
                        } catch (InterruptedException e) {
                            if(shutdown)break;
                        }
                    }
                    break;
                }
                default:
                    break;
            }

        }
    }

    private void startLeader(){
        roundRobinLeader =new RoundRobinLeader(logger,this,new ArrayList<>(peerIDtoAddress.keySet()));
        Thread leaderThread=new Thread(roundRobinLeader);
        leaderThread.setDaemon(true);
        leaderThread.start();
    }



    public class beatHart implements LoggingServer{
        public volatile ConcurrentHashMap<Long,Long> vervtorclock=new ConcurrentHashMap<>();
        final int GOSSIP = 1000;
         final int FAIL = 10;
        final int CLEANUP = FAIL * 2;
        public ConcurrentHashMap<Long,Boolean>dead=new ConcurrentHashMap<>();
        public ConcurrentHashMap<Long,Boolean>timedOut=new ConcurrentHashMap<>();
        volatile Boolean leader=false;
        PeerServerImpl peerServer;
        AtomicLong timeCount =new AtomicLong(0);
        Timer timer=new Timer(true);
        /*

        these are the methudeds for thread one of the hart beat
        the prosses will be
        1 update timer
        2 check for nodes to ignore
        3 set those nodes as dead
        4 clean up nodes
        5 send out clock info

         */
        public int update_timer() throws IOException {
            long out;
            out= timeCount.incrementAndGet();
            vervtorclock.put(id,out);
            return (int) out;

        }
        public boolean sendOutClock() throws IOException {


            List<Long> keys= new ArrayList<>(vervtorclock.keySet().stream().toList());
            ByteArrayOutputStream Bout =new ByteArrayOutputStream();
            ObjectOutputStream Oout=new ObjectOutputStream(Bout);
            Oout.writeObject((ConcurrentHashMap<Long, Long>)vervtorclock);
            Oout.close();
            keys.remove(peerServer.id);
            long tosend= keys.get((int) (timeCount.get() % keys.size()));
            long tosend2=keys.get((int) ((timeCount.get()+1) % keys.size()));
            long tosend3=keys.get((int) ((timeCount.get()+2) % keys.size()));
            if(keys.size()>3){
                peerServer.sendMessage(Message.MessageType.GOSSIP, Bout.toByteArray(), peerServer.getPeerByID(tosend));
                peerServer.sendMessage(Message.MessageType.GOSSIP, Bout.toByteArray(), peerServer.getPeerByID(tosend2));
                peerServer.sendMessage(Message.MessageType.GOSSIP, Bout.toByteArray(), peerServer.getPeerByID(tosend3));
            }else
                peerServer.sendBroadcast(Message.MessageType.GOSSIP,Bout.toByteArray());
            return true;
        }
        public List<Long>checkTimeForNodesToIgnore(){
            List<Long>out=new ArrayList<>();
            long time=timeCount.get();
            for (long i:vervtorclock.keySet()){

                if(time-vervtorclock.get(i)>FAIL){
                    out.add(i);
                peerServer.logger.info("dead is"+i);}
            }
            return out;
        }
        public boolean setAsdead(List<Long>todie){
            for(Long i:todie) {
                timedOut.put(i, true);
                dead.put(i,Boolean.TRUE);
                Vote leaderVote=peerServer.currentLeader;
                if(leaderVote!=null&&i==leaderVote.getProposedLeaderID()){
                    logger.info("newleder");
                }
                isalive.put(i,true);

            }
            return true;
        }

        public boolean cleanUp(){
            long time =timeCount.get();
            for(Long i: vervtorclock.keySet()){
                if(time - vervtorclock.get(i)>=CLEANUP){
                    vervtorclock.remove(i);
                }
            }
            return true;
        }

        public synchronized void beatAndCheck() throws IOException {
            //peerServer.logger.info("beat");
            update_timer();
            List<Long>died=checkTimeForNodesToIgnore();
            setAsdead(died);
            cleanUp();

        }


        /*
        the folowing is for the 2nd thread
        1 monitore for incoming gosip messeges
        2&3 when get messege:
            2 check what nodes to up date vector clock
            3 update vector clock

         */

        public ConcurrentHashMap<Long,Long>ReciveGosipes() throws InterruptedException, IOException, ClassNotFoundException {
            ConcurrentHashMap<Long,Long> incomeingClock=null;

                Message message=peerServer.Gossipe.poll(30000, TimeUnit.MILLISECONDS);
                if(message!=null) {
                    ByteArrayInputStream bin = new ByteArrayInputStream(message.getMessageContents());
                    ObjectInputStream oin = new ObjectInputStream(bin);
                    incomeingClock = (ConcurrentHashMap<Long, Long>) oin.readObject();
                }
                return incomeingClock;

        }

        public List<Long>WhatToUpDate(ConcurrentHashMap<Long,Long>gossip){

            ArrayList<Long>out=new ArrayList<>();
            try {


            for(Long ids:gossip.keySet()){
                boolean timed=false;
                if(timedOut.get(ids)!=null)
                    if(timedOut.get(ids)==true) {
                        timed=true;
                    }
                if(!timed){
                    if(vervtorclock.get(ids)!=null)
                        if(vervtorclock.get(ids)>=gossip.get(ids))
                            continue;
                    out.add(ids);
                }

            }}catch (Exception e){
                logger.warning(e.getMessage());
            }
            return out;
        }
        public boolean updateClock(List<Long> nodes,ConcurrentHashMap<Long,Long> values){
            for(Long node:nodes)
                vervtorclock.put(node,values.get(node));
            return true;
        }

        public void reciveBeats() throws IOException, InterruptedException, ClassNotFoundException {
            try {


                while (!shutdown) {
                    List<Long> updates;
                    ConcurrentHashMap<Long, Long> gosip = ReciveGosipes();

                    if (!(gosip == null))
                        synchronized (this) {
                            updates = WhatToUpDate(gosip);
                            updateClock(updates,gosip);
                        }
                }
            }catch (Exception e){
                logger.warning(e.getMessage());
                logger.warning(e.getLocalizedMessage());
            }

        }

        Logger logger;
        public beatHart(PeerServerImpl peerServer) throws IOException {

            this.peerServer=peerServer;
            logger =initializeLogging("server" +id+"gosip");
            for(Long ids: peerIDtoAddress.keySet()){
                vervtorclock.put(ids,0l);
            }
            vervtorclock.put(peerServer.id,0l);
            vervtorclock.put(peerServer.gatway,0l);
           Thread T= new Thread(new BeatsExspter());
           T.setDaemon(true);
           T.start();
            timer.schedule(new senderAnChecker(),0,GOSSIP);

        }
        class senderAnChecker extends TimerTask{

            @Override
            public void run() {
                if(shutdown)this.cancel();
                try {
                    logger.warning("437");
                    beatAndCheck();
                    sendOutClock();
                } catch (IOException e) {
                    peerServer.logger.warning(e.toString());
                    throw new RuntimeException(e);
                }

            }
        }
        class BeatsExspter implements Runnable{

            @Override
            public void run() {
                try {
                    reciveBeats();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        }





    }



}

