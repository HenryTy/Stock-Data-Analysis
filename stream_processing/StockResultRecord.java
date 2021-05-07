package com.example.bigdata;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StockResultRecord implements Serializable {

    private static final String LINE_PATTERN_STRING = "^\\d{4}-\\d{2}-\\d{2}T.*,(\\d+(\\.\\d+)?,){6}.+$";
    private static final Pattern LINE_PATTERN = Pattern.compile(LINE_PATTERN_STRING);

    private LocalDate date;
    private float open;
    private float high;
    private float low;
    private float close;
    private float adjClose;
    private float volume;
    private String stock;
    private String stockName;

    public StockResultRecord(LocalDate date, float open, float high, float low,
                             float close, float adjClose, float volume, String stock) {
        this.date = date;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.adjClose = adjClose;
        this.volume = volume;
        this.stock = stock;
        this.stockName = "Not Found";
    }

    public static boolean lineIsCorrect(String line) {
        Matcher m = LINE_PATTERN.matcher(line);
        return m.find();
    }

    public static StockResultRecord parseFromLine(String line) {
        String[] splittedLine = line.split(",");
        String dateWithoutTime = splittedLine[0].substring(0, 10);
        LocalDate date = LocalDate.parse(dateWithoutTime);
        Float[] floatData = new Float[6];
        for(int i=0; i<6; i++) {
            floatData[i] = Float.parseFloat(splittedLine[i+1]);
        }
        return new StockResultRecord(date, floatData[0], floatData[1], floatData[2],
                floatData[3], floatData[4], floatData[5], splittedLine[7]);
    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    public float getOpen() {
        return open;
    }

    public void setOpen(float open) {
        this.open = open;
    }

    public float getHigh() {
        return high;
    }

    public void setHigh(float high) {
        this.high = high;
    }

    public float getLow() {
        return low;
    }

    public void setLow(float low) {
        this.low = low;
    }

    public float getClose() {
        return close;
    }

    public void setClose(float close) {
        this.close = close;
    }

    public float getAdjClose() {
        return adjClose;
    }

    public void setAdjClose(float adjClose) {
        this.adjClose = adjClose;
    }

    public float getVolume() {
        return volume;
    }

    public void setVolume(float volume) {
        this.volume = volume;
    }

    public String getStock() {
        return stock;
    }

    public void setStock(String stock) {
        this.stock = stock;
    }

    public String getMonth() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM");
        return date.format(formatter);
    }

    public long getTimestampInMillis() {
        //milliseconds from 1960-01-01
        return (date.toEpochDay() + 3653) * 86400000;
    }

    @Override
    public String toString() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String dateString = date.format(formatter);
        if(stockName == null || stockName.equals("")) {
            stockName = "Not Found";
        }
        return String.format("%s;%f;%f;%f;%f;%f;%f;%s;%s", dateString, open, high, low, close,
                adjClose, volume, stock, stockName);
    }

    public static StockResultRecord fromString(String recordString) {
        String[] splittedLine = recordString.split(";");
        LocalDate date = LocalDate.parse(splittedLine[0]);
        Float[] floatData = new Float[6];
        for(int i=0; i<6; i++) {
            floatData[i] = Float.parseFloat(splittedLine[i+1]);
        }
        StockResultRecord record = new StockResultRecord(date, floatData[0], floatData[1], floatData[2],
                floatData[3], floatData[4], floatData[5], splittedLine[7]);
        record.setStockName(splittedLine[8]);
        return record;
    }


    public String getStockName() {
        return stockName;
    }

    public void setStockName(String stockName) {
        this.stockName = stockName;
    }
}