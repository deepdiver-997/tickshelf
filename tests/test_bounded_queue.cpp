#include <doctest/doctest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "components/engine/bounded_queue.hpp"

using namespace tickshelf;
using namespace std::chrono_literals;

TEST_CASE("BoundedQueue: 容量、FIFO 与关闭后排空") {
    BoundedQueue<int> queue(2);
    CHECK(queue.try_push(1));
    CHECK(queue.try_push(2));
    CHECK_FALSE(queue.try_push(3));
    CHECK(queue.size() == 2);

    int value = 0;
    CHECK(queue.pop_wait(value));
    CHECK(value == 1);
    CHECK(queue.try_push(3));

    queue.close();
    CHECK_FALSE(queue.try_push(4));
    CHECK(queue.pop_wait(value));
    CHECK(value == 2);
    CHECK(queue.pop_wait(value));
    CHECK(value == 3);
    CHECK_FALSE(queue.pop_wait(value));  // closed && drained
}

TEST_CASE("BoundedQueue: push_wait 被 pop 唤醒") {
    BoundedQueue<int> queue(1);
    REQUIRE(queue.try_push(1));
    std::atomic<bool> pushed{false};
    std::thread producer([&] { pushed = queue.push_wait(2); });

    // 等生产者真正阻塞在满队列上，再腾出空位。
    while (queue.size() != queue.capacity()) std::this_thread::yield();
    std::this_thread::sleep_for(10ms);

    int value = 0;
    CHECK(queue.pop_wait(value));
    CHECK(value == 1);
    producer.join();
    CHECK(pushed.load());
    CHECK(queue.pop_wait(value));
    CHECK(value == 2);
}

TEST_CASE("BoundedQueue: pop_wait_for 超时返回而不忙等") {
    BoundedQueue<int> queue(1);
    int value = 0;
    const auto start = std::chrono::steady_clock::now();
    CHECK_FALSE(queue.pop_wait_for(value, 5ms));
    CHECK(std::chrono::steady_clock::now() - start >= 1ms);
    CHECK(queue.try_push(7));
    CHECK(queue.pop_wait_for(value, 5ms));
    CHECK(value == 7);
}

TEST_CASE("BoundedQueue: close 唤醒阻塞的 pop_wait") {
    BoundedQueue<int> queue(1);
    std::atomic<int> result{-1};
    std::thread consumer([&] {
        int value = 0;
        result.store(queue.pop_wait(value) ? 1 : 0);
    });
    std::this_thread::sleep_for(10ms);
    queue.close();
    consumer.join();
    CHECK(result.load() == 0);
}
