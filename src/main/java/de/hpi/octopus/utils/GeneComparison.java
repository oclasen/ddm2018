package de.hpi.octopus.utils;

public class GeneComparison {

    public static int findLongestSubstring(String gene1, String gene2) {
        int m = gene1.length();
        int n = gene2.length();

        int max = 0;

        int[][] dp = new int[m][n];

        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                if (gene1.charAt(i) == gene2.charAt(j)) {
                    if (i == 0 || j == 0) {
                        dp[i][j] = 1;
                    } else
                        dp[i][j] = dp[i - 1][j - 1] + 1;
                } else {
                    dp[i][j] = 0;
                }

                if (max < dp[i][j])
                    max = dp[i][j];
            }
        }
        return max;

    }
}
