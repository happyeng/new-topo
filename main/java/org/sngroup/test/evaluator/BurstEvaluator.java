/*
 * This program is free software: you can redistribute it and/or modify it under the terms of
 *  the GNU General Public License as published by the Free Software Foundation, either
 *   version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *   PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this
 *  program. If not, see <https://www.gnu.org/licenses/>.
 *
 * Authors: Chenyang Huang (Xiamen University) <xmuhcy@stu.xmu.edu.cn>
 *          Qiao Xiang     (Xiamen University) <xiangq27@gmail.com>
 *          Ridi Wen       (Xiamen University) <23020211153973@stu.xmu.edu.cn>
 *          Yuxin Wang     (Xiamen University) <yuxxinwang@gmail.com>
 */

 package org.sngroup.test.evaluator;

 import net.sourceforge.argparse4j.inf.Namespace;
 import org.sngroup.Configuration;
 import org.sngroup.test.runner.Runner;
// import org.sngroup.util.Utility;
 //import org.sngroup.util.Recorder;
 
 import java.io.FileWriter;
 import java.util.LinkedList;
 import java.util.List;
 
 import java.io.File;
 import java.io.IOException;
 
 
 public class BurstEvaluator extends Evaluator{
     int times = 0;
 
     public BurstEvaluator setTimes(int times) {
         this.times = times;
         return this;
     }
 
     public BurstEvaluator(Namespace namespace) {
         super();
         setConfiguration(namespace);
         this.topology = namespace.getString("network");
         Configuration.getConfiguration().readDirectory(namespace.getString("network"), false);
         setTimes(namespace.getInt("times"));
     }
 
     @Override
     public void start(Runner runner){
         System.out.println("Start verification!!!");
         times = 1;

         long stime = System.currentTimeMillis();
         long bstime = System.currentTimeMillis();
         for (int i=0;i<times;i++) {
             bstime = System.currentTimeMillis();
             runner.build();
             stime = System.currentTimeMillis();
             runner.start();
             runner.awaitFinished();
             System.out.println("End Start in Runner!!!!!!!!!!");
             runner.close();
         }
         long etime = System.currentTimeMillis();
         long buildTime = stime - bstime;
         long verTime = etime - stime;
         long totalTime = etime - bstime;

         System.out.println("build time: " + buildTime + "ms\n");
         System.out.println("verification time: " + verTime + "ms\n");
         System.out.println("total time: " + totalTime + "ms\n");
         boolean test = true;
        //  while(test){}
         try{
             String data = topology  + "   " + buildTime
                     + ' ' + verTime + ' ' + totalTime + "ms\n";

             //true = append file
             FileWriter fileWritter = new FileWriter("./result/centralVer.csv",true);
             fileWritter.write(data);
             fileWritter.close();

         }catch(IOException e){
             e.printStackTrace();
         }


     }
 
 }