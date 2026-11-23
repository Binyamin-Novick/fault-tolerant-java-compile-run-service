package edu.yu.cs.com3800;

import edu.yu.cs.com3800.stage5.PeerServerImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**We are implemeting a simplfied version of the election algorithm. For the complete version which covers all possible scenarios, see https://github.com/apache/zookeeper/blob/90f8d835e065ea12dddd8ed9ca20872a4412c78a/zookeeper-server/src/main/java/org/apache/zookeeper/server/quorum/FastLeaderElection.java#L913
 */
public class LeaderElection implements LoggingServer {
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 300;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently 30 seconds.
     */
    private final static int maxNotificationInterval = 30000;
    private  LinkedBlockingQueue<Message> incomingMessages;
    PeerServer server;
    Logger logger;
    private long proposedEpoch;
    private long proposedLeader;
    Vote propose;
    Map<Long,ElectionNotification> idtovote=new HashMap<>();

    public LeaderElection(PeerServer server, LinkedBlockingQueue<Message> incomingMessages, Logger logger) throws IOException {
        this.server=server;
        ((PeerServerImpl)server).peerEpoch++;
        this.incomingMessages=incomingMessages;
        this.logger=initializeLogging(((PeerServerImpl) server).getName()+"election");
        propose=new ElectionNotification(server.getServerId(), PeerServer.ServerState.LOOKING,server.getServerId(),server.getPeerEpoch());
        proposedLeader=propose.getProposedLeaderID();
        proposedEpoch=propose.getPeerEpoch();
        logger.info("election init class complete");
    }

    /**
     * Note that the logic in the comments below does NOT cover every last "technical" detail you will need to address to implement the election algorithm.
     * How you store all the relevant state, etc., are details you will need to work out.
     * @return the elected leader
     */
    public synchronized Vote lookForLeader() {
        logger.info(server.getServerId()+"  election started");
        try {
            //send initial notifications to get things started
            sendNotifications();
            int interval=10;
            //Loop in which we exchange notifications with other servers until we find a leader
            while(!((PeerServerImpl)server).shutdown){
                if(((PeerServerImpl)server).shutdown)throw new InterruptedException();
                //Remove next notification from queue
                Message message=incomingMessages.poll(interval, TimeUnit.MILLISECONDS);
                //If no notifications received...
                if(message==null) {
                    //...resend notifications to prompt a reply from others
                    sendNotifications();
                    //...use exponential back-off when notifications not received but no longer than maxNotificationInterval...
                    interval = Math.min(interval * 2, maxNotificationInterval);
                    //If we did get a message...
                }else {
                    if(message.getMessageType()!= Message.MessageType.ELECTION)continue;
                    logger.info("server"+server.getServerId()+"  has messege"+message.toString());
                    ElectionNotification electionNotification= getNotificationFromMessage(message);
                    logger.info("server"+server.getServerId()+"  made notification  from message");
                    //...if it's for an earlier epoch, or from an observer, ignore it.
                    if(electionNotification.getPeerEpoch()<server.getPeerEpoch()||electionNotification.getState()== PeerServer.ServerState.OBSERVER){
                        logger.info("server"+server.getServerId()+"   notification is from earlier epoch "+electionNotification.getSenderID());
                        if(electionNotification.getState()== PeerServer.ServerState.OBSERVER){
                            //server.sendMessage(Message.MessageType.ELECTION,buildMsgContent(new ElectionNotification(proposedLeader,p)));
                        }
                        continue;}
                    //...if the received message has a vote for a leader which supersedes mine, change my vote (and send notifications to all other voters about my new vote).
                    if(supersedesCurrentVote(electionNotification.getProposedLeaderID(),electionNotification.getPeerEpoch())){
                        logger.info("server"+server.getServerId()+"  changing vote");
                        proposedEpoch=electionNotification.getPeerEpoch();
                        proposedLeader=electionNotification.getProposedLeaderID();
                        propose=electionNotification;
                        ElectionNotification temp=new ElectionNotification(propose.getProposedLeaderID(), PeerServer.ServerState.LOOKING,server.getServerId(),proposedEpoch);
                        server.sendBroadcast(message.getMessageType(), buildMsgContent(temp));
                    }else logger.info("server"+server.getServerId()+"  not changing vote");
                    //(Be sure to keep track of the votes I received and who I received them from.)
                    logger.info("server"+server.getServerId()+"  tracking messages");
                    idtovote.put(electionNotification.getSenderID(),electionNotification);



                }
                //If I have enough votes to declare my currently proposed leader as the leader...
                if(haveEnoughVotes(idtovote,propose)){
                    logger.info(propose.toString()+server.getServerId()+"  has enough to win");
                    //..do a last check to see if there are any new votes for a higher ranked possible leader. If there are, continue in my election "while" loop.
                    boolean arebetter=false;
                    logger.info(server.getServerId()+"  checking messages for better vote");
                    wait(finalizeWait);
                    while (incomingMessages.size()>0){
                        Message mempotot=incomingMessages.poll();
                        ElectionNotification checking= getNotificationFromMessage(mempotot);
                        if(supersedesCurrentVote(checking.getProposedLeaderID(),checking.getPeerEpoch())){
                            arebetter=true;
                            logger.info("server"+server.getServerId()+"  found better"+checking.getProposedLeaderID());
                        }

                    }
                    //If there are no new relevant message from the reception queue, set my own state to either LEADING or FOLLOWING and RETURN the elected leader.
                    if (!arebetter){
                        logger.info(" none are better \n election complete");

                        return acceptElectionWinner((ElectionNotification) propose);
                    }

                }

            }
        }
        catch (Exception e) {
            this.logger.log(Level.SEVERE,"Exception occurred during election; election canceled",e);
        }
        return null;
    }

    private Vote acceptElectionWinner(ElectionNotification n) {
        server.sendBroadcast(Message.MessageType.NEW_LEADER_GETTING_LAST_WORK,buildMsgContent(n));
        //set my state to either LEADING or FOLLOWING
        if(n.getProposedLeaderID()==server.getServerId()) {
            logger.info("server "+server.getServerId()+" setting this as leader");

            server.setPeerState(PeerServer.ServerState.LEADING);
        }else {
            logger.info("setting this as follower setting returning vote "+n.toString()+"as leader");
            server.setPeerState(PeerServer.ServerState.FOLLOWING);
        }

        //clear out the incoming queue before returning
        incomingMessages.clear();
        return n;
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
    }

    /**
     * Termination predicate. Given a set of votes, determines if we have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote.
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        //is the number of votes for the proposal > the size of my peer server’s quorum?
        votes.put(server.getServerId(),(ElectionNotification) this.propose);
        sendNotifications();
        return server.getQuorumSize()<votes.values().stream().filter((x)->x.getProposedLeaderID()== proposal.getProposedLeaderID()).collect(Collectors.toList()).size();
    }
    public static ElectionNotification getNotificationFromMessage(Message message){
        ByteBuffer buffer=ByteBuffer.wrap(message.getMessageContents());
        long leader = buffer.getLong();
        char stateChar = buffer.getChar();
        long senderID = buffer.getLong();
        long peerEpoch = buffer.getLong();
        ElectionNotification electionNotification=new ElectionNotification(leader, PeerServer.ServerState.getServerState(stateChar),senderID,peerEpoch);
        return electionNotification;//loock at 198 of messges
    }
    protected void sendNotifications(){
        logger.info("sending election notifications");
        propose=new ElectionNotification(proposedLeader, server.getPeerState(),server.getServerId(),propose.getPeerEpoch());
        server.sendBroadcast(Message.MessageType.ELECTION,buildMsgContent((ElectionNotification) propose));

    }

    public static byte[] buildMsgContent(ElectionNotification notification){
        ByteBuffer buffer =ByteBuffer.allocate(Long.BYTES*3+ Character.BYTES);
        buffer.putLong(notification.getProposedLeaderID());
        buffer.putChar(notification.getState().getChar());
        buffer.putLong(notification.getSenderID());
        buffer.putLong(notification.getPeerEpoch());
        return buffer.array();
    }
}