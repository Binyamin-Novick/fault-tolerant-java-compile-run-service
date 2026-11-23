package edu.yu.cs.com3800.stage3;

public class HappyWaitPath {
    public String run(){
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return "happywait   \n";
    }
}
