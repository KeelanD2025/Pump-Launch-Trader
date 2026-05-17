use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    High,
    Normal,
    Low,
}

#[derive(Debug, thiserror::Error)]
pub enum EventBusError {
    #[error("channel full")]
    Full,
    #[error("channel closed")]
    Closed,
}

#[derive(Debug)]
pub struct EventBus<T> {
    high_tx: mpsc::Sender<T>,
    high_rx: mpsc::Receiver<T>,
    normal_tx: mpsc::Sender<T>,
    normal_rx: mpsc::Receiver<T>,
    low_tx: mpsc::Sender<T>,
    low_rx: mpsc::Receiver<T>,
}

#[derive(Clone, Debug)]
pub struct EventPublisher<T> {
    high_tx: mpsc::Sender<T>,
    normal_tx: mpsc::Sender<T>,
    low_tx: mpsc::Sender<T>,
}

impl<T> EventBus<T> {
    pub fn bounded(capacity_per_priority: usize) -> Self {
        let (high_tx, high_rx) = mpsc::channel(capacity_per_priority);
        let (normal_tx, normal_rx) = mpsc::channel(capacity_per_priority);
        let (low_tx, low_rx) = mpsc::channel(capacity_per_priority);
        Self {
            high_tx,
            high_rx,
            normal_tx,
            normal_rx,
            low_tx,
            low_rx,
        }
    }

    pub fn publisher(&self) -> EventPublisher<T> {
        EventPublisher {
            high_tx: self.high_tx.clone(),
            normal_tx: self.normal_tx.clone(),
            low_tx: self.low_tx.clone(),
        }
    }

    pub fn len(&self) -> usize {
        self.high_rx.len() + self.normal_rx.len() + self.low_rx.len()
    }

    pub async fn recv(&mut self) -> Option<T> {
        if let Ok(item) = self.high_rx.try_recv() {
            return Some(item);
        }
        if let Ok(item) = self.normal_rx.try_recv() {
            return Some(item);
        }
        if let Ok(item) = self.low_rx.try_recv() {
            return Some(item);
        }
        tokio::select! {
            biased;
            item = self.high_rx.recv() => item,
            item = self.normal_rx.recv() => item,
            item = self.low_rx.recv() => item,
        }
    }
}

impl<T> EventPublisher<T> {
    pub fn try_publish(&self, priority: Priority, message: T) -> Result<(), EventBusError> {
        let sender = match priority {
            Priority::High => &self.high_tx,
            Priority::Normal => &self.normal_tx,
            Priority::Low => &self.low_tx,
        };
        sender.try_send(message).map_err(|error| match error {
            tokio::sync::mpsc::error::TrySendError::Closed(_) => EventBusError::Closed,
            tokio::sync::mpsc::error::TrySendError::Full(_) => EventBusError::Full,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{EventBus, Priority};

    #[tokio::test]
    async fn high_priority_preempts_lower_priority_messages() {
        let mut bus = EventBus::bounded(8);
        let publisher = bus.publisher();
        publisher
            .try_publish(Priority::Low, "low")
            .expect("low message");
        publisher
            .try_publish(Priority::High, "high")
            .expect("high message");
        assert_eq!(bus.recv().await, Some("high"));
        assert_eq!(bus.recv().await, Some("low"));
    }
}
