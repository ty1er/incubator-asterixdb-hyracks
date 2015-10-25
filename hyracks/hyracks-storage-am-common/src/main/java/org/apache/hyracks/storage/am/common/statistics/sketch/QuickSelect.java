package org.apache.hyracks.storage.am.common.statistics.sketch;

import java.util.Random;

public class QuickSelect {

    private static Random rand = new Random();

    private static int partition(double[] arr, int left, int right, int pivot) {
        double pivotVal = arr[pivot];
        swap(arr, pivot, right);
        int storeIndex = left;
        for (int i = left; i < right; i++) {
            if (arr[i] < pivotVal) {
                swap(arr, i, storeIndex);
                storeIndex++;
            }
        }
        swap(arr, right, storeIndex);
        return storeIndex;
    }

    public static double select(double[] arr, int n) {
        int left = 0;
        int right = arr.length - 1;
        while (right >= left) {
            int pivotIndex = partition(arr, left, right, rand.nextInt(right - left + 1) + left);
            if (pivotIndex == n) {
                return arr[pivotIndex];
            } else if (pivotIndex < n) {
                left = pivotIndex + 1;
            } else {
                right = pivotIndex - 1;
            }
        }
        return 0;
    }

    private static void swap(double[] arr, int i1, int i2) {
        if (i1 != i2) {
            double temp = arr[i1];
            arr[i1] = arr[i2];
            arr[i2] = temp;
        }
    }

}
