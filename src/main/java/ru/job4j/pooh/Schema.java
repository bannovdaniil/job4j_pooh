package ru.job4j.pooh;

import ru.job4j.pooh.model.Message;

public interface Schema extends Runnable {
    void addReceiver(Receiver receiver);

    void publish(Message message);
}
