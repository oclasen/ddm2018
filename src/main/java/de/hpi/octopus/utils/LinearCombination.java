package de.hpi.octopus.utils;

import java.util.ArrayList;
import java.util.Arrays;

public class LinearCombination {

    public ArrayList<ArrayList<Integer>> solve(ArrayList<Integer> numbers, long begin, long range) {
        ArrayList<ArrayList<Integer>> results = new ArrayList<>();

        for (long a = begin; a < range; a++) {
            String binary = Long.toBinaryString(a);
            binary = String.format("%1$" + numbers.size() + "s", binary)
                        .replace(' ', '0');

            Integer[] prefixes = new Integer[numbers.size()];
            for (int i = 0; i < prefixes.length; i++)
                prefixes[i] = 1;

            int i = 0;
            for (int j = binary.length() - 1; j >= 0; j--) {
                if (binary.charAt(j) == '1')
                    prefixes[i] = -1;
                i++;
            }

            if (this.sum(numbers, prefixes) == 0)
                results.add(new ArrayList<>(Arrays.asList(prefixes)));
        }
        return results;

    }

    private int sum(ArrayList<Integer> numbers, Integer[] prefixes) {
        int sum = 0;
        for (int i = 0; i < numbers.size(); i++)
            sum += numbers.get(i) * prefixes[i];
        return sum;
    }

}
