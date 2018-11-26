package de.hpi.ddm1HenschelClasen.utils;

import java.util.ArrayList;

public class LinearCombination {

    public int[] solve(ArrayList<Integer> input, long begin, long range) {

        int[] numbers = new int[input.size()];
        for (int i = 0; i < input.size(); i++) {
            numbers[i] = input.get(i);
        }

        for (long a = 0; a < range; a++) {
            String binary = Long.toBinaryString(begin);

            int[] prefixes = new int[numbers.length];
            for (int i = 0; i < prefixes.length; i++)
                prefixes[i] = 1;

            int i = prefixes.length-1;
            for (int j = binary.length()-1; j >= 0; j--) {
                if (binary.charAt(j) == '1')
                    prefixes[i] = -1;
                i--;
            }

            if (this.sum(numbers, prefixes) == 0) {
                return prefixes;
            }

            begin++;
        }

        return new int[0];
    }

    private int sum(int[] numbers, int[] prefixes) {
        int sum = 0;
        for (int i = 0; i < numbers.length; i++)
            sum += numbers[i] * prefixes[i];
        return sum;
    }

}