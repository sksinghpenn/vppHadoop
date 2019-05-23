import java.math.BigInteger;
import java.util.*;


public class Test {

    public static  boolean isPalindrome(String s) {

        if (s.trim().length() == 0) return true;

        String[] words = s.split(" ");

        StringBuilder fullWord = new StringBuilder("");
        for(String word: words) {
            fullWord.append(word.trim().toLowerCase());

        }

        String origWord = fullWord.toString();


        String reverseWord  = fullWord.reverse().toString();

        if (reverseWord.equals(origWord)) return true;

        return false;



    }

    public static int reverseBits(int n) {
        String binaryStr = Integer.toBinaryString(n);

        StringBuilder sb = new StringBuilder(binaryStr);
        String reversedStr = sb.reverse().toString();

        return new BigInteger(reversedStr, 2).intValue();

    }

    public static void main(String[] args) {
        System.out.println( reverseBits(1));
    }
}


