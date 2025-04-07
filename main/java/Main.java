

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

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.helper.HelpScreenException;
import net.sourceforge.argparse4j.inf.*;
import org.sngroup.Configuration;
import org.sngroup.test.evaluator.Evaluator;
import org.sngroup.test.evaluator.BurstEvaluator;
//import org.sngroup.test.evaluator.IncrementalEvaluator;
import org.sngroup.test.runner.*;

import java.math.BigInteger;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Main {
    public static void main(String[] args) {
        Logger logger = Logger.getGlobal();
        logger.setLevel(Level.INFO);
        ArgumentParser parser = ArgumentParsers
                .newFor("Tulkun").build()
                .defaultHelp(true)
                .description("Distributed Dataplane Verification");
        Subparsers subparser = parser.addSubparsers().title("subcommands").help("sub-command help").dest("prog").metavar("prog");
        Subparser cbs = subparser.addParser("cbs").help("Burst update simulation evaluator. All FIBs are read at once and then verified.");
        cbs.addArgument("network").type(String.class).help("Network name. All configurations will be set automatically.");
        cbs.addArgument("-t", "--times").type(Integer.class).setDefault(1).help("The times of burst update");
        Evaluator.setParser(cbs);
        Namespace namespace;
        try {
            namespace = parser.parseArgs(args);
        }catch (HelpScreenException e){
            return;
        } catch (ArgumentParserException e) {
            e.printStackTrace();
            return;
        }
        String prog = namespace.getString("prog");
        Evaluator evaluator;
        switch (prog) {
            case "cbs": {
                evaluator = new BurstEvaluator(namespace);
                evaluator.start(new TopoRunner());
                return;
            }
            case "list": {
                System.out.println("Network list:");
                for (String n : Configuration.getNetworkList()) {
                    System.out.println("\t"+n);
                }
            }
        }


    }
}
