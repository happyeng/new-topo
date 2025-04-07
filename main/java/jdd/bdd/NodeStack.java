package jdd.bdd;

import jdd.util.*;

import java.io.Serializable;

/**
 * Stack of nodes, temporary storage for nodes during garbage collection
 */
public final class NodeStack implements Cloneable, Serializable {
	public int tos;
	private int []stack;

	public NodeStack(){

	}

	@Override
	public Object clone() {
		NodeStack copy = null;
		try{
			copy = (NodeStack) super.clone();
		}catch(CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return copy;
	}


	public NodeStack(int size) {
		this.tos = 0;
		this.stack = new int[size];
	}
	public int push(int node) {
		stack[tos++] = node;
		return node;
	}
	public int pop() {
		return stack[--tos];
	}
	public void drop(int count) {
		tos -= count;
	}
	public void reset() {
		tos = 0;
	}
	public int getCapacity() {
		return stack.length;
	}
	public int getTOS() {
		return tos;
	}
	public int [] getData() {
		return stack;
	}
	public void grow(int newsize) {
		if(stack.length < newsize) {
			int [] newstack = Allocator.allocateIntArray(newsize);
			for(int i = 0; i < tos; i++)
				newstack[i] = stack[i];
			this.stack = newstack;
		}
	}
}