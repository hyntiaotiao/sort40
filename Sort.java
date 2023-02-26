package utils;

import java.io.*;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @projectName: sort40
 * @package: utils
 * @className: Main
 * @author: HanTiaoTiao
 * @description: TODO
 * @date: 2022/11/3 18:16
 * @version: 1.0
 */
public class Sort extends Thread {
    private int[] nums;
    private int count;
    File file;
    BufferedWriter writer;

    public Sort(int[] nums,int count){
        this.nums=nums;
        this.count=count;
        file=new File("D://sort"+count+".txt");
        try {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file,false)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        System.out.println("开始排序-"+count);
        Arrays.sort(nums);
        try {
            writer.write(Integer.toString(nums[0]));
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = 1; i < 20000000; i++) {
            try {
                writer.newLine();
                writer.write(Integer.toString(nums[i]));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //计数器减一
        MergeSort.latch.countDown();
        System.out.println("结束排序-"+count);
    }
}
