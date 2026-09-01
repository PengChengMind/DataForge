package org.apache.test;public class JavaTest { public static void main(String[] args) { int x = Integer.valueOf("101", 2);
        System.out.println(x); /* 你好*/ System.out.println();System.out.println(Integer.toBinaryString(x << 5));System.out.println(Integer.toBinaryString(x >> 32));

    }

}
