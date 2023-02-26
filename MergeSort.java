package utils;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @projectName: sort40
 * @package: utils
 * @className: MargeSort
 * @author: HanTiaoTiao
 * @description: TODO
 * @date: 2022/11/3 19:01
 * @version: 1.0
 */
public class MergeSort {
    //源数据存储位置
    static File file = new File("D:\\40亿.txt");
    //将40亿数据文件分200次读入内存
    static int count=200;
    //小根堆,（第二个位置用来标识该数来自哪个文件）
    static int[][] heap = new int[count+1][2];
    //堆的大小（大小是动态的，初始值设置为0）
    static int size = 0;
    static BufferedReader reader;
    static {
        try {
            reader = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    /*
     * CountDownLatch的构造函数接收一个 int 型参数作为计数器，例如想让N个任务完成之后才能继续执行，创建CountDownLatch时传入参数N；
     * 需要等待的线程会调用CountDownLatch的await方法，该线程就会阻塞直到计数器的值减为0，
     * 而达到自己预期的线程会调用CountDownLatch的countDown()方法，计数器N的值减1。
     * 由于countDown方法可以在任何地方调用，所以计数器N既可以代表N个线程，也可以是一个线程的N个执行步骤。
     */
    public final static CountDownLatch latch = new CountDownLatch(count);

    public static void startSort() throws IOException, InterruptedException {
        //创建一个具有固定大小的线程池（线程数量通常设置为本机CPU数量的俩倍）
        ExecutorService service = Executors.newFixedThreadPool(8);
        long startTime =  System.currentTimeMillis();//开始计时
        //循环读入40亿数据，每次读入俩千万个数，最后生成200个排好序的小文件
        for (int i = 0; i < count; i++) {
            int[] nums = new int[20000000];
            for (int j = 0; j < 20000000; j++) {
                nums[j]=Integer.parseInt(reader.readLine());
            }
            //启动排序线程（对本次读入的俩千万个数进行排序）
            Sort sort=new Sort(nums,i+1);
            service.execute(sort);
        }
        reader.close();
        //阻塞当前线程（等待子线程完成所有排序任务后主线程开始归并）
        latch.await();
        System.out.println("开始归并");
        //将200个文件中的数归并，完成最终排序
        mergeSort();
        long endTime =  System.currentTimeMillis();//计时结束
        long usedTime = (endTime-startTime)/1000;
        System.out.println("归并完成");
        System.out.println("共用时："+usedTime+"秒");
    }

    public static void mergeSort() throws IOException {
        File[] files=new File[count];
        BufferedReader[] readers=new BufferedReader[count];
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("E:\\sorted.txt",false)));

        //读取200个文件
        for (int i = 0; i < count; i++) {
            files[i]=new File("D:\\sort"+(i+1)+".txt");
            readers[i] = new BufferedReader(new FileReader(files[i]));
        }

        //先读取各个文件的最小值，并将其存入小根堆
        for (int i = 0; i < count; i++) {
            //存数
            heap[++size][0]=Integer.parseInt(readers[i].readLine());
            //存该数所在的文件index
            heap[size][1]=i;
            //将该数放到小根堆中正确的位置
            up(size);
        }


        while (size>0){
            writer.write(Integer.toString(heap[1][0]));
            writer.newLine();
            int next = heap[1][1]; //下一个要读取的文件的下标
            //将首部元素和尾部元素交换，并将size减一（实现逻辑上删除根节点）
            heap[1][0]=heap[size][0];
            heap[1][1]=heap[size][1];
            size--;
            //将交换后的首部元素放到正确的位置
            down(1);
            //将对应文件的下一个数读入小根堆
            if(readers[next].ready()) {
                heap[++size][0]=Integer.parseInt(readers[next].readLine());
                heap[size][1]=next;
                up(size);
            }
        }

        //关闭流
        writer.close();
        for (int i = 0; i < count; i++) {
            readers[i].close();
        }
    }

    public static void down(int x){
        int t=x;
        if(2*t<=size&&heap[2*t][0]<heap[x][0]) x=2*t;
        if(2*t+1<=size&&heap[2*t+1][0]<heap[x][0]) x=2*t+1;
        if(t!=x){
            int temp=heap[t][0];
            heap[t][0]=heap[x][0];
            heap[x][0]=temp;

            temp=heap[t][1];
            heap[t][1]=heap[x][1];
            heap[x][1]=temp;
            down(x);
        }
    }

    public static void up(int x){
        int t=x;
        if(x/2>0&&heap[x/2][0]>heap[x][0]) x=x/2;
        if(x!=t){
            int temp=heap[t][0];
            heap[t][0]=heap[x][0];
            heap[x][0]=temp;

            temp=heap[t][1];
            heap[t][1]=heap[x][1];
            heap[x][1]=temp;
            up(x);
        }
    }
}
