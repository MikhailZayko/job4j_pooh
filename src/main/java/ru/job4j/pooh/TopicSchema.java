package ru.job4j.pooh;

import java.util.concurrent.*;

public class TopicSchema implements Schema {

    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Receiver>> receivers = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, BlockingQueue<String>> data = new ConcurrentHashMap<>();

    private final Condition condition = new Condition();

    @Override
    public void addReceiver(Receiver receiver) {
        receivers.putIfAbsent(receiver.name(), new CopyOnWriteArrayList<>());
        receivers.get(receiver.name()).add(receiver);
        condition.on();
    }

    @Override
    public void publish(Message message) {
        data.putIfAbsent(message.name(), new LinkedBlockingQueue<>());
        data.get(message.name()).add(message.text());
        condition.on();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            for (String queueKey : receivers.keySet()) {
                BlockingQueue<String> queue = data.getOrDefault(queueKey, new LinkedBlockingQueue<>());
                CopyOnWriteArrayList<Receiver> receiversList = receivers.get(queueKey);
                while (!receiversList.isEmpty()) {
                    String data = queue.poll();
                    if (data == null) {
                        break;
                    }
                    receiversList.forEach(receiver -> receiver.receive(data));
                }
            }
            condition.off();
            try {
                condition.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
