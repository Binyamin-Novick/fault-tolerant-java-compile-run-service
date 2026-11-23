package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.stage5.GatewayServer;
import edu.yu.cs.com3800.stage5.PeerServerImpl;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PeerServerImplTest {
    public class severs {
        public PeerServerImpl[] peerServers;
        public GatewayServer gat;

        public void start() {
            gat.start();
            for (PeerServerImpl server : peerServers)
                server.start();
        }
    }

    int v = 2500;
    int myPort = 8080;
    private InetSocketAddress myAddress = new InetSocketAddress("localhost", this.myPort);


    private severs makeServersWithSameEpoch(int amount) throws IOException {
        PeerServerImpl[] peerServers = new PeerServerImpl[amount];
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        for (int i = 0; i < amount; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", i * 3 + v));
        }
        peerIDtoAddress.put(42l, new InetSocketAddress("localhost", 9999));
        int i = 0;
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            if(entry.getKey()!=42){
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            PeerServerImpl server = new PeerServerImpl(entry.getValue().getPort(), 5, entry.getKey(), map, 42l);
            if (Math.toIntExact(server.getServerId()) != 42)
                peerServers[Math.toIntExact(server.getServerId())] = server;
        }}
        v = v + amount + 1;
        severs sever = new severs();
        sever.peerServers = peerServers;
        peerIDtoAddress.remove(42L);
        sever.gat = new GatewayServer(8888, 9999, 0, 42l, new ConcurrentHashMap<>(peerIDtoAddress), 1);
        return sever;
    }

    public HttpURLConnection makeHttp() throws IOException {
        return (HttpURLConnection) new URL("http://localhost:8888/compileandrun").openConnection();
    }

    // Method to read input from a file
    public String getInput(String path) throws IOException {
        return Files.readString(Paths.get(path), StandardCharsets.UTF_8);
    }

    // Method to send a POST request with Java source code for compilation and execution
    public void sendCompileAndRunRequest(String src, HttpURLConnection httpClient) throws IOException {
        try {
            httpClient.setDoOutput(true);
            httpClient.setRequestMethod("POST");
            httpClient.setRequestProperty("Content-type", "text/x-java-source");

            try (OutputStream os = httpClient.getOutputStream()) {
                os.write(src.getBytes(StandardCharsets.UTF_8));
                os.flush();
            }
        } catch (IOException e) {
            System.err.println("Error sending compile and run request: " + e.getMessage());
            e.printStackTrace();
        }
    }
    @Test
    public void tt2() throws IOException, InterruptedException {
        severs severs = makeServersWithSameEpoch(3);
        severs.start();
        sleep(5000);
        severs.peerServers[0].shutdown();
        while (true) ;
    }


    @Test
    public void ttt() throws IOException {
        severs severs = makeServersWithSameEpoch(4);
        severs.start();
        while (true) ;
    }
    @Test
    public void ttt2() throws IOException, InterruptedException {
        severs severs = makeServersWithSameEpoch(4);
        severs.start();
        sleep(5000);
        severs.peerServers[0].shutdown();
        while (true) ;
    }

    // Test method to perform a basic happy path test

    @Test
    public void basicHappy() throws IOException, InterruptedException {
        // Simulating a server setup (this might need further implementation)
        severs servers = makeServersWithSameEpoch(2); // Assuming this method is defined elsewhere
        servers.start();
        Thread.sleep(4000);
        // Create the HTTP client for communication
        HttpURLConnection httpClient = makeHttp();
        //httpClient.setReadTimeout(5000);

        // Read Java source code from a file
        String in = getInput("src/test/java/edu/yu/cs/com3800/stage3/HappyPath.java");

        // Send the request to compile and run the source code
        sendCompileAndRunRequest(in, httpClient);

        // Wait for the response (you can improve this logic to be more reliable)


        // Get the HTTP response code and handle accordingly
        int responseCode = httpClient.getResponseCode();
        if (responseCode == 200) {
            try (InputStream inputStream = httpClient.getInputStream()) {
                // Print out the response (compiled and run result)
                String response = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                assertEquals(response, "happy path\n");
            }
        } else {
            System.err.println("Request failed with response code: " + responseCode);
        }
        for (PeerServerImpl ps : servers.peerServers) {
            ps.shutdown();
        }
        servers.gat.gatewayPeerServer.shutdown();

    }


    @Test
    public void shutdownLeader() throws IOException, InterruptedException {
        // Simulating a server setup (this might need further implementation)
        severs servers = makeServersWithSameEpoch(4); // Assuming this method is defined elsewhere
        servers.start();

        Thread.sleep(15000);
        servers.peerServers[3].shutdown();
        Thread.sleep(30000);
        // Create the HTTP client for communication
        HttpURLConnection httpClient = makeHttp();
        httpClient.setReadTimeout(15000);

        // Read Java source code from a file
        String in = getInput("src/test/java/edu/yu/cs/com3800/stage3/HappyPath.java");

        // Send the request to compile and run the source code
        sendCompileAndRunRequest(in, httpClient);

        // Wait for the response (you can improve this logic to be more reliable)


        // Get the HTTP response code and handle accordingly
        int responseCode = httpClient.getResponseCode();
        assertEquals(responseCode ,200);
            try (InputStream inputStream = httpClient.getInputStream()) {
                // Print out the response (compiled and run result)
                String response = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                assertEquals(response,"happy path\n");
            }

        for(PeerServerImpl ps:servers.peerServers){
            ps.shutdown();
        }
        servers.gat.gatewayPeerServer.shutdown();

    }
    /**
     * leader gets the job, gives it to a follower, then dies while the follower is still running it.
     * The follower must hand the result to the NEW leader, which forwards it to the gateway.
     */
    @Test
    public void shutdownLeaderWhileFollowerWorking() throws Exception {
        severs servers = makeServersWithSameEpoch(4);
        servers.start();
        Thread.sleep(15000);

        String in = getInput("src/test/java/edu/yu/cs/com3800/stage3/HappyWaitPath.java"); //runs for 5 seconds
        String[] response = new String[1];
        int[] code = new int[1];
        Throwable[] error = new Throwable[1];
        Thread client = new Thread(() -> {
            try {
                HttpURLConnection httpClient = makeHttp();
                httpClient.setReadTimeout(60000);
                sendCompileAndRunRequest(in, httpClient);
                code[0] = httpClient.getResponseCode();
                try (InputStream inputStream = httpClient.getInputStream()) {
                    response[0] = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                }
            } catch (Throwable t) {
                error[0] = t;
            }
        });
        client.start();
        Thread.sleep(2500); //job is now on a follower, still running
        servers.peerServers[3].shutdown(); //kill the leader
        client.join(70000);

        if (error[0] != null) throw new AssertionError("request failed", error[0]);
        assertEquals(200, code[0]);
        assertEquals("happywait   \n", response[0]);

        for (PeerServerImpl ps : servers.peerServers) ps.shutdown();
        servers.gat.gatewayPeerServer.shutdown();
    }

@Test
    public void chachingwithhappy() throws IOException, InterruptedException {
    severs servers = makeServersWithSameEpoch(4); // Assuming this method is defined elsewhere
    servers.start();
    Thread.sleep(4000);
    // Create the HTTP client for communication
    HttpURLConnection httpClient = makeHttp();
    // httpClient.setReadTimeout(5000);

    // Read Java source code from a file
    String in = getInput("src/test/java/edu/yu/cs/com3800/stage3/HappyPath.java");

    // Send the request to compile and run the source code
    sendCompileAndRunRequest(in, httpClient);

    // Wait for the response (you can improve this logic to be more reliable)


    // Get the HTTP response code and handle accordingly
    int responseCode = httpClient.getResponseCode();
   assertEquals(200,responseCode);
        try (InputStream inputStream = httpClient.getInputStream()) {
            // Print out the response (compiled and run result)
            String response = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(response,"happy path\n");
        }

    httpClient.disconnect();
        httpClient=makeHttp();
    httpClient.setReadTimeout(650);
    sendCompileAndRunRequest(in, httpClient);

    // Wait for the response (you can improve this logic to be more reliable)


    // Get the HTTP response code and handle accordingly
    responseCode = httpClient.getResponseCode();
    assertEquals( 200,responseCode);
        try (InputStream inputStream = httpClient.getInputStream()) {
            // Print out the response (compiled and run result)
            String response = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(response,"happy path\n");
        }

}
@Test
    public void vve() throws IOException, ClassNotFoundException {
    HashMap<Long, Long> originalMap = new HashMap<>();
    originalMap.put(1L, 100L);
    originalMap.put(2L, 200L);
    originalMap.put(3L, 300L);

    // Serialize to byte array
    byte[] byteArray = serializeHashMap(originalMap);
    System.out.println("Serialized Byte Array: " + byteArray.length + " bytes");

    // Deserialize back to HashMap
    HashMap<Long, Long> deserializedMap = deserializeHashMap(byteArray);
    System.out.println("Deserialized HashMap: " + deserializedMap);
}

    // Serialize a HashMap to a byte array
    public static byte[] serializeHashMap(HashMap<Long, Long> map) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
        objectStream.writeObject(map);
        objectStream.close();
        return byteStream.toByteArray();
    }

    // Deserialize a byte array back to a HashMap

    public static HashMap<Long, Long> deserializeHashMap(byte[] byteArray) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(byteArray);
        ObjectInputStream objectStream = new ObjectInputStream(byteStream);
        return (HashMap<Long, Long>) objectStream.readObject();
    }
}








