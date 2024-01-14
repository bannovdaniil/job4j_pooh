package ru.job4j.pooh;

import ru.job4j.pooh.model.Message;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Режим темы
 * <p>
 * Рассмотри так же пример из трех клиентов: один поставщик, два потребителя.
 * Поставщик создает события, потребитель их получает.
 * Представим, что поставщик отправил два сообщения в очередь: ["message 1", "message 2"].
 * Если у нас два потребителя, то каждый из них должен получить все сообщения от поставщика,
 * То есть потребитель 1 получить сообщения: "message 1", "message 2".
 * и потребитель 2 тоже получить сообщения: "message 1", "message 2".
 * <p>
 * То есть в режиме темы все сообщения от поставщика должны быть доставлены каждому потребителю.
 */
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
            for (Map.Entry<String, CopyOnWriteArrayList<Receiver>> receiver : receivers.entrySet()) {
                sendMessageToTopic(receiver.getKey(), receiver.getValue());
            }
            condition.off();
            try {
                condition.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Берем сообщение, и проходимся по очереди получателей, пока они не закончатся.
     * Получаем следующее сообщение, и заново идем по очереди получателей.
     */
    private void sendMessageToTopic(String topicName, CopyOnWriteArrayList<Receiver> receiversByQueue) {
        BlockingQueue<String> messageQueue = data.getOrDefault(topicName, new LinkedBlockingQueue<>());
        Iterator<Receiver> receiverIterator = receiversByQueue.iterator();

        String message = messageQueue.poll();

        while (receiverIterator.hasNext()) {
            if (message != null) {
                receiverIterator.next().receive(message);
            }
            if (message == null) {
                break;
            }
            if (!receiverIterator.hasNext()) {
                message = messageQueue.poll();
                receiverIterator = receiversByQueue.iterator();
            }
        }
    }
}
