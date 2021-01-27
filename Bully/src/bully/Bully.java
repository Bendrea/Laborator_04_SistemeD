/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bully;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.HashMap;


/**
 *
 * @author alexb
 */
public class Bully implements Runnable {

    /**
     * @param args the command line arguments
     */
     static int leader_id = -1;
    static int self_id = -1;
    static int server_Port = 5678;
    String operation;
    String reqtype;
    static int source_id = -1;
    static HashMap<Integer, String> processes = new HashMap();
    static boolean received = false;
    static long start_time = -1;
    static boolean leader_flag = false;
    static boolean election_req = false;
    static int higher;
    static int ok_ctr = 0;
    static long start_time_ok = -1;
 
    
    public Bully(String operation) {
        this.operation = operation;
    }

    public Bully(String operation, String reqtype) {
        this.operation = operation;
        this.reqtype = reqtype;
    }
    
    
   public static void main(String args[]) throws UnknownHostException, IOException, InterruptedException {
        // TODO code application logic here
        initialize();
        Runnable receiver =  (Runnable) new Bully("receiver");
        new Thread(receiver).start();

        Thread.sleep(10);
        Runnable heartbeat = (Runnable) new Bully("heartbeat");
        new Thread(heartbeat).start();

        Runnable timer = (Runnable) new Bully("timer");
        new Thread(timer).start();

        Runnable timerok = (Runnable) new Bully("timerok");
        new Thread(timerok).start();

        while (true) {
        }
    }
   
    public void run() {




        if (operation.equals("timer")) {
            System.out.println("Inside timer thread");
            try {
                Thread.sleep(7000);
                //System.out.println("Timer thread Awake");
                if (!received) {
                    leader_id = self_id;
                    election_req = false;              // to allow another election req to come in
                    leader_flag = true;
                    System.out.println("******I am the selected LEADER ! " + leader_id);
                    Runnable sender = (Runnable) new Bully("sender", "cood");
                    new Thread(sender).start();

                }

                if (received && !leader_flag) {
                    System.out.println("Received OK but  LEADER HAS FAILED, leaderflag= " + leader_flag);

                    election_req = false;
                    received = false;
                    System.out.println("election_req= " + election_req + " received = " + received);

                    Runnable sender = (Runnable) new Bully("sender", "elecreq");
                    new Thread(sender).start();

                }
            } catch (Exception e) {
                System.out.println("Interrupted in Timer Thread");

            }




        } else if (operation.equals("timerok")) {
            System.out.println("Inside timerOK thread");
            while (true) {
                if ((!leader_flag) && System.currentTimeMillis() - start_time_ok > (5000 + (5000 * (5 - self_id)))) {
                    ok_ctr = 0;
                    System.out.println("Higher Process Sent OK but Failed, so Start a new Election process");
                    Runnable sender = (Runnable) new Bully("sender", "elecreq");
                    new Thread(sender).start();
                    break;

                }

            }













        } else if (operation.equals("receiver")) {
            ServerSocket serverSocket = null;

            try {
                serverSocket = new ServerSocket(server_Port);
                while (true) {
                    Socket socket = serverSocket.accept();
                    //System.out.println("Connection established.....");
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    String option = in.readUTF();
                    if (option.equals("elecreq")) {

                        source_id = Integer.parseInt(in.readUTF());
                        System.out.println("received election request from " + source_id);
                        if (self_id > source_id) {
                            Runnable sender = (Runnable) new Bully("sender", "ok");
                            new Thread(sender).start();

                        }

                        if (!election_req) {
                            Runnable sender = (Runnable) new Bully("sender", "elecreq");
                            new Thread(sender).start();
                            start_time = System.currentTimeMillis();
                            election_req = true;

                            Runnable timer = (Runnable) new Bully("timer");
                            new Thread(timer).start();
                            System.out.println("Start time here is :" + start_time);
                        }

                    } else if (option.equals("ok")) {
                        received = true;
                        int sender = Integer.parseInt(in.readUTF());
                        System.out.println("Received OK from " + processes.get(sender));

                    } else if (option.equals("cood")) {

                        leader_id = Integer.parseInt(in.readUTF());
                        leader_flag = true;
                        election_req = false;
                        received = false;
                        System.out.println("******LEADER selected is :" + leader_id + " : " + processes.get(leader_id) + " Leader Flag= " + leader_flag);
                        System.out.println("Election Request value=  " + election_req + " Value of received= " + received);

                    } else if (option.equals("heartbeat")) {
                        int sender = Integer.parseInt(in.readUTF());
                        System.out.println("HEARTBEAT received from " + processes.get(sender));

                    }

                    socket.close();
                }

            } catch (Exception e) {
                e.printStackTrace();

            }





        } else if (operation.equals("heartbeat")) {
            while (true) {
                try {

                    System.out.print("");
                    if (leader_flag && (self_id != leader_id)) {
                        Thread.sleep(1500);
                        String destination_server = (String) processes.get(leader_id);
                        Socket socket = new Socket(destination_server, server_Port);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("heartbeat");
                        out.writeUTF(self_id + "");

                        System.out.println("Sent HEARTBEAT to : " + destination_server);

                    }

                } catch (Exception e) {
                    leader_flag = false;

                    System.out.println("Leader has FAILED!");
                    System.out.println("election_req= " + election_req + " received = " + received);
                    //send election request
                    Runnable sender = (Runnable) new Bully("sender", "elecreq");
                    new Thread(sender).start();

                }
            }

        } else if (operation.equals("sender")) {

            if (reqtype.equals("elecreq")) {
                try {
                    sendElectionRequest();

                } catch (Exception e) {

                    System.out.println("Process has FAILED, Election Request cannot be processed !");

                }
            } else if (reqtype.equals("ok")) {
                try {
                    sendOK();
                } catch (Exception e) {
                    e.printStackTrace();

                }

            } else if (reqtype.equals("cood")) {
                try {
                    sendCoordinatorMsg();

                } catch (Exception e) {

                    System.out.println("Process has FAILED, Won't get the new leader !");

                }
            }

        }

    }
      public static void sendCoordinatorMsg() throws IOException {
        for (int key : processes.keySet()) {
            if (key != self_id) {
                String destination_server = (String) processes.get(key);
                try {
                    Socket socket = new Socket(destination_server, server_Port);
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    out.writeUTF("cood");
                    out.writeUTF(leader_id + "");

                    System.out.println("Sent Leader ID to : " + destination_server);
                } catch (Exception e) {
                    System.out.println("The process " + destination_server + " has failed, won't get the new leader !");
                }

            }
        }
    }

    /**
     * The sendOK() method sends OK message to the incoming process which has
     * requested an election request.
     *
     */
    public static void sendOK() throws IOException {
        try {
            String destination_server = (String) processes.get(source_id);
            Socket socket = new Socket(destination_server, server_Port);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeUTF("ok");
            out.writeUTF(self_id + "");
            System.out.println("Sent OK to : " + destination_server);
        } catch (Exception e) {
            System.out.println("Process " + processes.get(source_id) + " has FAILED. OK Message cannot be sent !");
        }
    }

    /**
     * The sendElectionRequest() method sends Election Request to all the higher
     * processes.
     *
     */
    public static void sendElectionRequest() throws IOException {
        System.out.println("Election Initiated..");
        int failure = 0;
        for (int key : processes.keySet()) {
            if (key > self_id) {
                String destination_server = (String) processes.get(key);
                try {
                    Socket socket = new Socket(destination_server, server_Port);
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    out.writeUTF("elecreq");
                    out.writeUTF(self_id + "");

                    System.out.println("Sent Election Request to : " + destination_server);
                } catch (Exception e) {
                    System.out.println("The process :" + destination_server + " has FAILED, cannot send Election Request !");
                    failure++;
                }
            }
        }
        if (failure == higher) {
            if (!election_req) {
                start_time = System.currentTimeMillis();
                System.out.println("Inside if of sendElectionRequest, start_time= " + start_time);
                election_req = true;
                received = false;
                Runnable timer = (Runnable) new Bully("timer");
                new Thread(timer).start();
            }
        }

    }




    /**
     * The initialize() method makes all the initializations and one of the
     * processes starts the election algorithm.
     *
     */
    public static void initialize() throws UnknownHostException, IOException {

	processes.put(1, "192.168.1.100");
        processes.put(2, "192.168.1.100");
        processes.put(3, "192.168.1.100");


        //InetAddress iAddress = InetAddress.getLocalHost();
        //String server_name = iAddress.getHostAddress();
	String server_name=null;

        Enumeration en = NetworkInterface.getNetworkInterfaces();
        while (en.hasMoreElements()) {
            NetworkInterface ni = (NetworkInterface) en.nextElement();
            Enumeration ee = ni.getInetAddresses();
            while (ee.hasMoreElements()) {
                InetAddress ia = (InetAddress) ee.nextElement();
                if(ia.getHostAddress().compareTo("192.168.1.")>0)
                    //System.out.println(ia.getHostAddress());
		    server_name = ia.getHostAddress();
            }
        }

        if (server_name.equals("192.168.1.100")) {
            self_id = 1;
            System.out.println("Server id here is= " + self_id);
            higher = 2;

        } else if (server_name.equals("192.168.1.100")) {
            self_id = 2;
            System.out.println("Server id here is= " + self_id);
            higher = 1;

        } else if (server_name.equals("192.168.1.100")) {
            self_id = 3;
            System.out.println("Server id here is= " + self_id);
            higher = -1;
            Runnable sender = (Runnable) new Bully("sender", "elecreq");
            new Thread(sender).start();

        }
    }



    
}
