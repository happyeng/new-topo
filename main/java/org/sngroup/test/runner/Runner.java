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

package org.sngroup.test.runner;

import org.sngroup.util.Network;
import org.sngroup.Configuration;
import org.sngroup.util.DevicePort;
import org.sngroup.util.ThreadPool;
import org.sngroup.verifier.BDDEngine;
import org.sngroup.verifier.Context;
import org.sngroup.verifier.Device;

import java.util.Set;

public abstract class Runner {

    Network network;
//    Collection<Event> events;

    public Runner() {
        this.network = Configuration.getConfiguration().genNetwork();
    }

    public Runner(int kvalue) {
        this.network = new Network();
        System.out.println("Debug fattree" + kvalue);
    }


    abstract public void build();

    abstract public void start();

    abstract public void awaitFinished();

    abstract public void sendCount(Context ctx, DevicePort sendPort, BDDEngine bddEngine);

//    abstract public void sendCountFromNet(Context ctx, DevicePort sendPort);

//    abstract public void sendCountByTopo(Context ctx, DevicePort sendPort, BDDEngine bddEngine, Set<Device> visited);

    abstract public void close();

    abstract public long getInitTime();

    abstract public Device getDevice(String s);

    abstract public ThreadPool getThreadPool();
}
