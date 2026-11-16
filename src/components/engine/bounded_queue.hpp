#pragma once
// 每分片一个有界队列（mutex+cv，SPSC 使用：源线程 push / 分片 worker pop）。
//
// 设计口径（回应 ADR-020 附录）：这里确实是异步交接，队列会出现真实深度，
// 所以锁竞争与背压都需要被显式处理；队列元素是指针尺寸的 SlicePtr，tick 数据
// 不过队列。cv 使用谓词循环 + notify_one，空闲时零唤醒；年龄封口/重试退避/
// 退出 drain 由调用方的 timed wait 负责，不做主动 sleep。
//
// close() 后：try_push 失败，pop_wait 把剩余元素排空后返回 false。
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <utility>

namespace tickshelf {

template <typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t capacity) : capacity_(capacity == 0 ? 1 : capacity) {}
    BoundedQueue(const BoundedQueue&) = delete;
    BoundedQueue& operator=(const BoundedQueue&) = delete;

    // 非阻塞：满或已关闭返回 false。
    bool try_push(T value) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (closed_ || items_.size() >= capacity_) return false;
            items_.push_back(std::move(value));
        }
        not_empty_.notify_one();
        return true;
    }

    // 阻塞直到有空位；关闭返回 false（调用方保留 value 的语义由调用方决定）。
    bool push_wait(T value) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            not_full_.wait(lock, [&] { return closed_ || items_.size() < capacity_; });
            if (closed_) return false;
            items_.push_back(std::move(value));
        }
        not_empty_.notify_one();
        return true;
    }

    // 阻塞直到有空位或超时；关闭/超时返回 false。
    bool push_wait_for(T value, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!not_full_.wait_for(lock, timeout,
                                [&] { return closed_ || items_.size() < capacity_; }))
            return false;
        if (closed_) return false;
        items_.push_back(std::move(value));
        lock.unlock();
        not_empty_.notify_one();
        return true;
    }

    // 阻塞直到有元素；关闭且排空后返回 false。
    bool pop_wait(T& out) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [&] { return closed_ || !items_.empty(); });
        if (items_.empty()) return false;
        out = std::move(items_.front());
        items_.pop_front();
        lock.unlock();
        not_full_.notify_one();
        return true;
    }

    // 超时版本：worker 用来兼顾 stop/统计，不忙等。
    bool pop_wait_for(T& out, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!not_empty_.wait_for(lock, timeout, [&] { return closed_ || !items_.empty(); }))
            return false;
        if (items_.empty()) return false;
        out = std::move(items_.front());
        items_.pop_front();
        lock.unlock();
        not_full_.notify_one();
        return true;
    }

    void close() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    bool closed() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return closed_;
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return items_.size();
    }

    std::size_t capacity() const { return capacity_; }

private:
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::deque<T> items_;
    const std::size_t capacity_;
    bool closed_ = false;
};

}  // namespace tickshelf
