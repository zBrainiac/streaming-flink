package consumer;

import java.util.concurrent.atomic.AtomicLong;

public class GetNextNumber {
    private static AtomicLong sequenceNumber = new AtomicLong(000000000000L);

    public static long getNextCode() {
        Long code = sequenceNumber.getAndIncrement();
        if (code == 999999999999L) {
            sequenceNumber = new AtomicLong(000000000000L);
            code = sequenceNumber.getAndIncrement();
        }
        System.out.println("//*****Next Sequence Number*****// Code" + code);
        return code;
    }
}

