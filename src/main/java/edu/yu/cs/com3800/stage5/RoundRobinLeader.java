package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.stage5.JavaRunnerFollower;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class RoundRobinLeader implements Runnable{
    PeerServerImpl peerServer ;
    LinkedBlockingQueue<Message> received;
    LinkedBlockingQueue<Message> send;
    List<Long> peerSeverIDs;
    public Long upto=0l;
    Logger logger;
    ServerSocket serverSocket;

    LinkedBlockingQueue<Long>jobstoSend=new LinkedBlockingQueue<>();
    HashMap<Long,Message>Jmesseges=new HashMap<>();

    HashMap<Long,Long>JobsToWorker=new HashMap<>();
    HashMap<Long,List<Long>>WorkersToJobs=new HashMap<>();



    public RoundRobinLeader(Logger logger,PeerServerImpl peerServer,List<Long> peerSeverIDs){
        this.logger=logger;
        logger.info( "server "+peerServer.getName()+"  initializing ring leader");
        this.peerServer=peerServer;

        //workers = everyone except the gateway and dead peers. Must be mutable: workerfail() removes from it
        this.peerSeverIDs=new ArrayList<>(peerSeverIDs.stream().filter((x)-> !Objects.equals(x, peerServer.gatway) && !peerServer.isPeerDead(x)).toList());
        try {
            serverSocket=new ServerSocket(peerServer.getUdpPort()+2);
            //serverSocket.setSoTimeout(75);
        }catch (Exception e){
            logger.warning(e.toString());
        }

        for (Long id :peerSeverIDs){
            if(!Objects.equals(id, peerServer.gatway))
                WorkersToJobs.put(id,new ArrayList<>());
        }

    }
    public void shutdown(){
        try {
            if(serverSocket!=null) serverSocket.close();
        } catch (IOException e) {
            logger.warning(e.getMessage());
        }
    }
    public synchronized void  workerfail(List<Long> died) throws InterruptedException {
        for(Long l:died)
            peerSeverIDs.remove(l);
        reAllocWorkers(died,new ArrayList<feedback>());

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
    @Override
    public void run() {
        Socket socket;
        Message message;
        feedback feedback=new feedback();
        int path=0;
        logger.info( "server "+peerServer.getName()+"  starting ring leader");
        while (!Thread.interrupted()) {
            if(serverSocket==null||serverSocket.isClosed()||peerServer.shutdown)return;
            feedback.value=0;
            message=recieveMessges(feedback);
            if(Thread.interrupted())return;
            if(feedback.value==0){
                logger.info("got-job");
                synchronized (this){
                    getMessgesType(message,feedback);
                    if(feedback.value==2){

                        freeJob(message.getRequestID(), feedback);
                        path=2;

                }else if(feedback.value==1){
                        Allocjob(feedback,message);
                        path=1;

                    }
                }
            if(path==2){
                sendToGateWay(feedback,message);
            } else if (path==1) {
                sendTofollower(feedback, message.getRequestID());
            }
            }
        }

    }
    public Message recieveMessges(feedback feedback){
        Socket socket;
        InputStream byststreem=null;
        try {
            socket=serverSocket.accept();

        } catch (IOException e) {
            logger.warning(e.getMessage());
            feedback.value=1;
            return null;
        }
        try {
            byststreem=socket.getInputStream();
        } catch (IOException e) {
            logger.warning(e.getMessage());
            feedback.value=2;
            return null;
        }
        if(peerServer.shutdown){
            Thread.currentThread().interrupt();
        }
        Message message=new Message(Util.readAllBytesFromNetwork(byststreem));
        if(message.getMessageType()== Message.MessageType.WORK||message.getMessageType()== Message.MessageType.COMPLETED_WORK){
            feedback.value=0;
        }else feedback.value=3;
        return message;





    }


    public void getMessgesType(Message message ,feedback feedback){
        feedback.value=0;
        if(message.getMessageType()== Message.MessageType.COMPLETED_WORK)
            feedback.value=2;
        if(message.getMessageType()== Message.MessageType.WORK)
            feedback.value=1;

    }
    public void freeJob(Long job,feedback feedback){
        feedback.value=0;
        Message work =Jmesseges.get(job);
        if(work==null){
            feedback.value=1;

        }else {
            Jmesseges.remove(job);
        }
        Long worker =JobsToWorker.get(job);
        if(worker==null){
            feedback.value= feedback.value+2;
            boolean was=false;
            for(Long w:WorkersToJobs.keySet()){
                for(int i=0;i<WorkersToJobs.get(w).size();i++){
                    if(Objects.equals(job, WorkersToJobs.get(w).get(i))){
                        WorkersToJobs.get(w).remove(i);
                        was=true;
                    }
                }

            }
            if(!was){
                feedback.value=feedback.value+4;
            }
        }else{

            JobsToWorker.remove(job);
            if(!WorkersToJobs.get(worker).remove(job))
                feedback.value=feedback.value+4;
        }



    }
    public void sendToGateWay(feedback feedback,Message completed){
        feedback.value=0;
        InetSocketAddress res=new InetSocketAddress(peerServer.getPeerByID(peerServer.gatway).getAddress(),
                peerServer.getPeerByID(peerServer.gatway).getPort()+2);
        Message tosend =peerServer.sendtcpmessge(Message.MessageType.COMPLETED_WORK, completed.getMessageContents(),
                    res,completed.getRequestID());


        try {
            Socket socketOut=new Socket(tosend.getReceiverHost(), tosend.getReceiverPort());
            OutputStream out =socketOut.getOutputStream();
            out.write(tosend.getNetworkPayload());
            out.flush();
        } catch (IOException e) {
            feedback.value=-1;
            logger.warning(e.getMessage());

        }

    }
    public void sendTofollower(feedback feedback,long job){
        feedback.value=0;
        long follower = JobsToWorker.get(job);
        InetSocketAddress res=new InetSocketAddress(peerServer.getPeerByID(follower).getAddress(),peerServer.getPeerByID(follower).getPort()+2);
        Message tosend= peerServer.sendtcpmessge(Message.MessageType.WORK,Jmesseges.get(job).getMessageContents(),res,job);
        try {
            Socket socketOut=new Socket(tosend.getReceiverHost(), tosend.getReceiverPort());
            OutputStream out =socketOut.getOutputStream();
            out.write(tosend.getNetworkPayload());
            out.flush();
        } catch (IOException e) {
            feedback.value=-1;
            logger.warning(e.getMessage());

        }
    }
    public void Allocjob(feedback feedback,Message message){
        feedback.value=0;
        if(message.getMessageType()!= Message.MessageType.WORK){
            feedback.value=1;
        }
        upto++;


      long worker =peerSeverIDs.get((int) (upto%peerSeverIDs.size()));

        Jmesseges.put(message.getRequestID(), message);
        JobsToWorker.put(message.getRequestID(), worker);
        WorkersToJobs.get(worker).add(message.getRequestID());
        jobstoSend.offer(message.getRequestID());


    }
    public void reAllocWorkers(List<Long>followers,List<feedback>feedbacks){
            feedback fed;
            for(Long follower:followers){
                fed=new feedback();
                feedbacks.add(fed);
                reAlloc(follower,fed);
            }
    }
    public void reAlloc(Long follower,feedback feedback){
        feedback.value=0;
        Message message;
        List<Long>jobs=WorkersToJobs.get(follower);
        if(jobs==null) {
            feedback.value = 2;
            return;
        }else{
            WorkersToJobs.remove(follower);
            if(jobs.isEmpty()){

                return;
            }else{
                for(long job:jobs){
                    JobsToWorker.remove(job);
                    Allocjob(feedback,Jmesseges.get(job));

                }
            }
        }
    }





    public class sockinfo implements Comparable<sockinfo>{
        String host;
        int port;
        public sockinfo(String host, int port){
            this.host=host;
            this.port=port;
        }
        public String tostring(){
            return host +port;
        }




        @Override
        public int compareTo(sockinfo o) {
            if(host.compareTo(o.host)!=0)return host.compareTo(o.host);
            if(o.port>port)return -1;
            if(o.port<port)return 1;
            return 0;
        }
    }
}
