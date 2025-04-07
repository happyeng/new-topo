/*
 * Atomic Predicates Verifier
 *
 * Copyright (c) 2013 UNIVERSITY OF TEXAS AUSTIN. All rights reserved. Developed
 * by: HONGKUN YANG and SIMON S. LAM http://www.cs.utexas.edu/users/lam/NRL/
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * with the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimers.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimers in the documentation
 * and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the UNIVERSITY OF TEXAS AUSTIN nor the names of the
 * developers may be used to endorse or promote products derived from this
 * Software without specific prior written permission.
 *
 * 4. Any report or paper describing results derived from using any part of this
 * Software must cite the following publication of the developers: Hongkun Yang
 * and Simon S. Lam, Real-time Verification of Network Properties using Atomic
 * Predicates, IEEE/ACM Transactions on Networking, April 2016, Volume 24, No.
 * 2, pages 887-900 (first published March 2015, Digital Object Identifier:
 * 10.1109/TNET.2015.2398197).
 *
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS WITH
 * THE SOFTWARE.
 */

package org.sngroup.util;

import com.google.common.net.InetAddresses;
import org.sngroup.verifier.TSBDD;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.logging.Logger;

public class Utility {
	private static int IDCount = 0;
	private static Vector<Integer> ZeroVector = null;
	private static final Logger logger = Logger.getLogger("log");

	public static Vector<Integer> getOneNumVector(int num){
		Vector<Integer> t = new Vector<>(1);
		t.add(num);
		return t;
	}

	public static long Power2(int exponent)
	{
		if(exponent <=16)
		{
			switch(exponent){
				case 0: return 1;
				case 1: return 2;
				case 2: return 4;
				case 3: return 8;
				case 4: return 16;
				case 5: return 32;
				case 6: return 64;
				case 7: return 128;
				case 8: return 256;
				case 9: return 512;
				case 10: return 1024;
				case 11: return 2048;
				case 12: return 4096;
				case 13: return 8192;
				case 14: return 16384;
				case 15: return 32768;
				case 16: return 65536;
				default: System.err.println("exponent is too large!");
					break;
			}
		}
		else
		{
			long power = 1;
			for(int i = 0; i < exponent; i ++)
			{
				power = power * 2;
			}
			return power;
		}
		// should not be here
		return 0;
	}



	/**
	 * return the binary representation of num
	 * e.g. num = 10, bits = 4, return an array of {0,1,0,1}
	 */
	public static int[] CalBinRep(long num, int bits)
	{
		if(bits == 0) return new int[0];

		int [] binrep = new int[bits];
		long numtemp = num;
		for(int i = bits; i >0; i--)
		{
			long abit = numtemp & Power2(i - 1);
			if(abit == 0)
			{
				binrep[i - 1] = 0;
			}else
			{
				binrep[i - 1] = 1;
			}
			numtemp = numtemp - abit;
		}
		return binrep;
	}

	// 转化ipv6地址
	public static int[] ipv6ToBinaryArray(String ipv6Address, int bits) throws UnknownHostException{
		// 将ipv6地址解析为InetAddress对象
		InetAddress inetAddress = InetAddress.getByName(ipv6Address);

		// 获取ipv6地址的字节数组
		byte[] ipAddressBytes = inetAddress.getAddress();

		// 创建一个长度为128的整数数组, 用于存储二进制表示
		int[] binaryArray = new int[bits];
		int k = ipAddressBytes.length;

		// 将字节数组中的每个字节转为对应的二进制位
		for(int i = 0; i < ipAddressBytes.length; i++){
			byte b = ipAddressBytes[i];
			for(int j = 0; j < 8; j++){
				binaryArray[(k -i - 1) * 8 + (7 - j)] = (b >> (7 - j)) & 1;
			}
		}
		return binaryArray;
	}

	public static String charToInt8bit(char[] c, int start){
		if(c.length < start+7) return "";
		int result = 0;
		for(int i=0, a=128; i<8; i++, a/=2){
			if(c[start+i] == '1'){
				result += a;
			}
		}
		return String.valueOf(result);
	}
	public static String charToInt(char[] c, int start, int length){
		if(c.length < start+length-1) return "";
		long result = 0;
		for(int i=1, a=1; i<=length; i++, a*=2){
			if(c[start+length-i] == '1'){
				result += a;
			}
		}
		return String.valueOf(result);
	}

	public static void main(String[] args){
	}

}
