#ifndef MESSAGE_QUEUE_H_
#define MESSAGE_QUEUE_H_

#include <mutex>
#include <queue>
using namespace std;

#include "Headers.hpp"

template <class T>
class MessageQueue {
   public:
    T Get();
    void Push(T msg);
    size_t Size() {
        return m_messages.size();
    }
    bool Empty() {
        return m_messages.empty();
    }

   private:
    queue<T> m_messages;    // Message content
    mutex m_mtx;            // Mutex for synchronization
    Condition m_condition;  // Condition variable for synchronization
};

template <class T>
inline T MessageQueue<T>::Get() {
    while (m_messages.empty()) {
        m_condition.Wait();
    }

    m_mtx.lock();
    auto msg = m_messages.front();
    m_messages.pop();
    m_mtx.unlock();

    return msg;
}

template <class T>
inline void MessageQueue<T>::Push(T msg) {
    m_mtx.lock();
    m_messages.push(msg);
    m_mtx.unlock();
    m_condition.Notify();
}

#endif