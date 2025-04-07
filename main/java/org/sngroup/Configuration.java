package org.sngroup;

import org.sngroup.util.Network;
import org.sngroup.util.Pair;

//import javafx.print.PrintColor;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Logger;

public class Configuration {
    static private final Configuration configuration = new Configuration();

    public static Configuration getConfiguration() {
        return configuration;
    }

    static String dir="config/";

    private String ruleFile;

    private String topologyFile;

    private String spaceFile;

    private boolean showResult;

    private boolean useOneThreadOneDpvnet;

    private int threadPoolSize;

    private Configuration() {
        setShowResult(false);
        setUseOneThreadOneDpvnet(true);
        setThreadPoolSize(40);
    }

    public void setRuleFile(String ruleFile) {
        this.ruleFile = ruleFile;
    }

    public void setTopologyFile(String topologyFile) {
        this.topologyFile = topologyFile;
    }

    public void setSpaceFile(String spaceFile) {
        this.spaceFile = spaceFile;
    }

    public String getSpaceFile() {
        return spaceFile;
    }

    public String getDeviceRuleFile(String device){
        String path = this.ruleFile;

        return path+((path.endsWith("/")?"":"/")+device);
    }
    public static void main(String[] args) {

    }

    public Network genNetwork(){
        Network network = new Network();
        network.readTopologyByFile(topologyFile);
        network.readDstDevices(topologyFile);
        network.readEdgeDevices(topologyFile);
        return network;
    }

    static public List<String> getNetworkList(){
        File directory = new File(dir);
        return Arrays.asList(directory.list());
    }

    public void readDirectory(String networkName, boolean isIncrementalRule){
        System.out.println(System.getProperty("user.dir"));
        String dirname = dir+networkName;
        File file = new File(dirname);
        System.out.println("Read configuration in: " + dirname);
        if(file.isDirectory()){
            Configuration configuration = Configuration.getConfiguration();
            File topologyFile = new File(dirname+"/"+"topology");
            String spaceFilePath = configuration.getSpaceFile();
            String ruleFilePath = configuration.ruleFile;
            if(spaceFilePath == null) spaceFilePath = dirname + "/" + "packet_space";
            if(ruleFilePath == null) ruleFilePath = dirname + "/rule/";
            File spaceFile = new File(spaceFilePath);
            File ruleFile = new File(ruleFilePath);

            if(topologyFile.isFile() && spaceFile.isFile() && ruleFile.isDirectory()){
                configuration.setTopologyFile(topologyFile.getAbsolutePath());
                configuration.setSpaceFile(spaceFile.getAbsolutePath());
                configuration.setRuleFile(ruleFile.getAbsolutePath()+"/");
            }else{
                System.out.println("File is not exists in: ");
                System.out.println(topologyFile);
                System.out.println(spaceFile);
                System.out.println(ruleFile);
            }
        }
        System.out.println("Finish read configuration in: " + dirname);
    }


    public boolean isShowResult() {
        return showResult;
    }

    public void setShowResult(boolean showResult) {
        this.showResult = showResult;
    }
    public void setUseOneThreadOneDpvnet(boolean useOneThreadOneDpvnet) {
        this.useOneThreadOneDpvnet = useOneThreadOneDpvnet;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

}
