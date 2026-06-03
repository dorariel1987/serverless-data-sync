import time

from datasync.queues.memory import InMemoryQueue


def test_send_receive_increments_dequeue_count():
    q = InMemoryQueue()
    q.send("hello")
    msgs = q.receive()
    assert len(msgs) == 1
    assert msgs[0].body == "hello"
    assert msgs[0].dequeue_count == 1


def test_receive_respects_max_messages():
    q = InMemoryQueue()
    for i in range(5):
        q.send(str(i))
    assert len(q.receive(max_messages=2)) == 2
    assert q.approximate_count() == 3


def test_delayed_message_not_visible_immediately():
    q = InMemoryQueue()
    q.send("later", delay_seconds=0.2)
    assert q.receive() == []
    time.sleep(0.25)
    msgs = q.receive()
    assert len(msgs) == 1 and msgs[0].body == "later"


def test_empty_queue_returns_empty_list():
    assert InMemoryQueue().receive() == []
