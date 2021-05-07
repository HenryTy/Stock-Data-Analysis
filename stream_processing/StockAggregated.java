package com.example.bigdata;

import java.io.Serializable;

public class StockAggregated implements Serializable {

    private int closeCount = 0;
    private float closeSum = 0;
    private float lowMin = -1;
    private float highMax = 0;
    private float volumeSum = 0;

    public StockAggregated(int closeCount, float closeSum, float lowMin, float highMax, float volumeSum) {
        this.closeCount = closeCount;
        this.closeSum = closeSum;
        this.lowMin = lowMin;
        this.highMax = highMax;
        this.volumeSum = volumeSum;
    }

    public StockAggregated(float lowMin, float highMax) {
        this.lowMin = lowMin;
        this.highMax = highMax;
    }

    public void update(StockResultRecord newRecord) {
        closeCount++;
        closeSum += newRecord.getClose();
        volumeSum += newRecord.getVolume();
        updateMinMax(newRecord);
    }

    public void updateMinMax(StockResultRecord newRecord) {
        lowMin = lowMin == -1 ? newRecord.getLow() : Math.min(lowMin, newRecord.getLow());
        highMax = Math.max(highMax, newRecord.getHigh());
    }

    public float getMinMaxDiff() {
        return (highMax - lowMin) / highMax;
    }

    public int getCloseCount() {
        return closeCount;
    }

    public void setCloseCount(int closeCount) {
        this.closeCount = closeCount;
    }

    public float getCloseSum() {
        return closeSum;
    }

    public void setCloseSum(float closeSum) {
        this.closeSum = closeSum;
    }

    public float getLowMin() {
        return lowMin;
    }

    public void setLowMin(float lowMin) {
        this.lowMin = lowMin;
    }

    public float getHighMax() {
        return highMax;
    }

    public void setHighMax(float highMax) {
        this.highMax = highMax;
    }

    public float getVolumeSum() {
        return volumeSum;
    }

    public void setVolumeSum(float volumeSum) {
        this.volumeSum = volumeSum;
    }

    @Override
    public String toString() {
        return String.format("%d;%f;%f;%f;%f", closeCount, closeSum, lowMin, highMax, volumeSum);
    }

    public static StockAggregated fromString(String stockAggString) {
        String[] splitted = stockAggString.split(";");
        return new StockAggregated(Integer.parseInt(splitted[0]),
                Float.parseFloat(splitted[1]), Float.parseFloat(splitted[2]),
                Float.parseFloat(splitted[3]), Float.parseFloat(splitted[4]));
    }
}
