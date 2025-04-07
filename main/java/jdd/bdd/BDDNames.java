
package jdd.bdd;

import jdd.util.*;

import java.io.Serializable;

/** BDD-style node naming: v1..vn */
public class BDDNames implements NodeName, Cloneable, Serializable {
	public BDDNames() { }

	public String zero() { return "FALSE"; }
	public String one() { return "TRUE"; }
	public String zeroShort() { return "0"; }
	public String oneShort() { return "1"; }

	@Override
	public Object clone() {
		BDDNames copy = null;
		try{
			copy = (BDDNames) super.clone();
		}catch(CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return copy;
	}


	public String variable(int n) {
		if(n < 0) return "(none)";
		return String.format("v%d", n + 1);
	}
}
