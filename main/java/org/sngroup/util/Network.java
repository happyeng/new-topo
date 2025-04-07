package org.sngroup.util;

import org.sngroup.verifier.Node;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

public class Network {

    public static void main(String[] args) {
    }

    final public Map<String, Map<String, Set<DevicePort>>> devicePorts; // S A S->A
    public static Map<DevicePort, DevicePort> topology;

    public Set<String> dstDevices;

    public Set<String> edgeDevices;
    boolean isNodeUsed = false;


    public Network(){
        topology = new Hashtable<>();
        devicePorts = new Hashtable<>();
        dstDevices = new HashSet<>();
        edgeDevices = new HashSet<>();
    }

    public void addTopology(String d1, String p1, String d2, String p2) {
        addDevice(d1);
        addDevice(d2);
        DevicePort dp1 = new DevicePort(d1, p1);
        DevicePort dp2 = new DevicePort(d2, p2);
        topology.put(dp1, dp2);
        topology.put(dp2, dp1);
        Node.topology.put(dp1, dp2);
        Node.topology.put(dp2, dp1);
        devicePorts.get(d1).putIfAbsent(d2, new HashSet<>());
        devicePorts.get(d2).putIfAbsent(d1, new HashSet<>());
        devicePorts.get(d1).get(d2).add(dp1);
        devicePorts.get(d2).get(d1).add(dp2);
    }

    public void addDevice(String name){
        if (devicePorts.containsKey(name)) return;
        devicePorts.put(name, new HashMap<>());
    }

    public void readDstDevices(String topologyFilePath){
        String filePath = topologyFilePath.replace("topology", "dstDevices");
        try {
            File file = new File(filePath);
            Scanner scanner = new Scanner(file);
            while (scanner.hasNextLine()) {
                String dstDevice = scanner.nextLine();
                dstDevices.add(dstDevice);
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            System.err.println("File not found: " + filePath);
            e.printStackTrace();

        }
        // System.out.println(dstDevices);
    }

    public void readEdgeDevices(String topologyFilePath){
        String filePath = topologyFilePath.replace("topology", "edgeDevices");
        try {
            File file = new File(filePath);
            Scanner scanner = new Scanner(file);
            while (scanner.hasNextLine()) {
                String dstDevice = scanner.nextLine();
                edgeDevices.add(dstDevice);
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            System.err.println("File not found: " + filePath);
            e.printStackTrace();

        }
        // System.out.println(edgeDevices);
    }

    public void readTopologyByFile(String filepath){

        try {
            InputStreamReader isr = new InputStreamReader(Files.newInputStream(Paths.get(filepath)), StandardCharsets.UTF_8);
            BufferedReader br = new BufferedReader(isr);
            String line;
            String[] token;
            while ((line = br.readLine()) != null) {
                token = line.split("\\s+");
                addTopology(token[0], token[1], token[2], token[3]);
            }
            isr.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

