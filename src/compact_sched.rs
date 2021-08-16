use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::{Rc, Weak};
use std::time::Duration;

use glommio::timer::TimerActionRepeat;
use glommio::{Local, TaskQueueHandle};

use crate::error::Result;
use crate::level::Levels;
use crate::types::LevelId;

crate trait CompactScheduler {
    fn enqueue(&self);
}

crate struct QueueUpCompSched {
    is_compacting: RefCell<bool>,
    interval: Duration,
    queue: RefCell<VecDeque<LevelId>>,
    delay_num: usize,
    levels: Weak<Levels>,
    tq: TaskQueueHandle,
}

impl QueueUpCompSched {
    crate fn new(
        interval: Duration,
        delay_num: usize,
        levels: Weak<Levels>,
        tq: TaskQueueHandle,
    ) -> Self {
        Self {
            is_compacting: RefCell::new(false),
            interval,
            queue: RefCell::new(VecDeque::new()),
            delay_num,
            levels,
            tq,
        }
    }

    crate fn enqueue(&self, l_id: LevelId) {
        self.queue.borrow_mut().push_back(l_id);
    }

    crate async fn schedule(self: Rc<Self>) -> Option<Duration> {
        if *self.is_compacting.borrow() || self.queue.borrow().len() < self.delay_num {
            return Some(self.interval);
        }

        let level_id = self.queue.borrow_mut().pop_front().unwrap();
        *self.is_compacting.borrow_mut() = true;

        let levels = self.levels.clone();
        Local::local_into(
            async move {
                // todo: propagate Error?
                let _ = levels.upgrade().unwrap().compact_level(level_id);
            },
            self.tq,
        )
        .unwrap()
        .detach();

        Some(self.interval)
    }

    crate fn install(self: Rc<Self>, tq: TaskQueueHandle) -> Result<()> {
        let sched = self.clone();
        TimerActionRepeat::repeat_into(move || sched.clone().schedule(), tq)?;

        Ok(())
    }

    /// For writing mock test.
    ///
    /// # Panic
    /// `levels` in the returning object is not initialize (an empty `Weak`).
    /// Any operations make this to call `levels` will panic due to
    /// the attempt of trying to upgrade that empty weak pointer.
    #[cfg(test)]
    crate fn default() -> Self {
        Self {
            is_compacting: RefCell::new(false),
            interval: Duration::from_secs(1),
            queue: RefCell::new(VecDeque::new()),
            delay_num: 3,
            levels: Weak::new(),
            tq: Local::create_task_queue(
                glommio::Shares::default(),
                glommio::Latency::NotImportant,
                "test_comp_tq",
            ),
        }
    }
}
