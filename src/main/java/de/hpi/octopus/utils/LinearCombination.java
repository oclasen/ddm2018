package de.hpi.octopus.utils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;

public class LinearCombination {

    private ArrayList<Integer> passwords;
    private long range;
    private BigInteger bigIntSigns;

    public LinearCombination(ArrayList<Integer> passwords, BigInteger bigIntSigns, long range) {
        this.passwords = passwords;
        this.bigIntSigns = bigIntSigns;
        this.range = range;
    }

    public ArrayList<ArrayList<Integer>> calc() {
        long start = System.nanoTime();

        String signs;
        ArrayList<String> resultsString = new ArrayList<>();
        for(long i = 0; i<= range; i++) {
            signs = bigIntSigns.toString(2);
            char[] leading = new char[passwords.size() - signs.length()];
            Arrays.fill(leading, '0');
            signs = new String(leading) + signs;

            long linearResult = 0;
            for(int j = 0; j<passwords.size(); j++) {
                linearResult += (long) Math.pow(-1, (int) signs.charAt(j)) * passwords.get(j);
            }
            if(linearResult == 0) {
                resultsString.add(signs);
            }
            bigIntSigns = bigIntSigns.add(BigInteger.ONE);
        }

        ArrayList<ArrayList<Integer>> results = new ArrayList<>();
        for (String s : resultsString) {
            ArrayList<Integer> prefix = new ArrayList<>();
            for (int i = 0; i < s.length(); i++) {
                if (s.charAt(i)== '1') {
                    prefix.add(-1);
                } else {
                    prefix.add(1);
                }
            }
            results.add(prefix);
        }

        System.out.println(System.nanoTime() - start);
        return results;
    }
}
