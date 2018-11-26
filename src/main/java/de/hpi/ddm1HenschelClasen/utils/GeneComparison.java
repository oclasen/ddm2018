package de.hpi.ddm1HenschelClasen.utils;

public class GeneComparison {

    public static int findLongestSubstring(String gene1, String gene2) {
        if (!gene1.equals(gene2)) {
//            int m = gene1.length();
//            int n = gene2.length();
//
//            int max = 0;
//
//            int[][] dp = new int[m][n];
//
//            for (int i = 0; i < m; i++) {
//                for (int j = 0; j < n; j++) {
//                    if (gene1.charAt(i) == gene2.charAt(j)) {
//                        if (i == 0 || j == 0) {
//                            dp[i][j] = 1;
//                        } else
//                            dp[i][j] = dp[i - 1][j - 1] + 1;
//                    } else {
//                        dp[i][j] = 0;
//                    }
//
//                    if (max < dp[i][j])
//                        max = dp[i][j];
//                }
//            }
//            return max;
//        }
//        return 0;
//
//        if (gene1.isEmpty() || gene2.isEmpty())
//            return 0;
//
//        if (gene1.length() > gene2.length()) {
//            String temp = gene1;
//            gene1 = gene2;
//            gene2 = temp;
//        }

            int[] currentRow = new int[gene1.length()];
            int[] lastRow = gene2.length() > 1 ? new int[gene1.length()] : null;
            int longestSubstringLength = 0;
            int longestSubstringStart = 0;

            for (int str2Index = 0; str2Index < gene2.length(); str2Index++) {
                char str2Char = gene2.charAt(str2Index);
                for (int str1Index = 0; str1Index < gene1.length(); str1Index++) {
                    int newLength;
                    if (gene1.charAt(str1Index) == str2Char) {
                        newLength = str1Index == 0 || str2Index == 0 ? 1 : lastRow[str1Index - 1] + 1;

                        if (newLength > longestSubstringLength) {
                            longestSubstringLength = newLength;
                            longestSubstringStart = str1Index - (newLength - 1);
                        }
                    } else {
                        newLength = 0;
                    }
                    currentRow[str1Index] = newLength;
                }
                int[] temp = currentRow;
                currentRow = lastRow;
                lastRow = temp;
            }
            return longestSubstringLength;

        }
        return 0;
    }
}
