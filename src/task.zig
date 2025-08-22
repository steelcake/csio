const std = @import("std");
const Instant = std.time.Instant;
const linux = std.os.linux;

const slab_mod = @import("./slab.zig");
const Slab = slab_mod.Slab;
const SlabKey = slab_mod.Key;
const IoAlloc = @import("./io_alloc.zig").IoAlloc;
const SliceMap = @import("./slice_map.zig").SliceMap;
const Queue = @import("./queue.zig").Queue;

pub const MAX_IO_PER_TASK = 128;

pub const TaskEntry = struct {
    task: Task,
    finished_io: [MAX_IO_PER_TASK]linux.io_uring_cqe,
    num_finished_io: u8,
    num_pending_io: u8,
};

pub const Context = struct {
    start_t: Instant,
    task_id: u32,
    task_entry: *TaskEntry,
    io_queue: *Queue(linux.io_uring_sqe),
    polled_io_queue: *Queue(linux.io_uring_sqe),
    io: *Slab(SlabKey),
    to_notify: *SliceMap(SlabKey, void),
    preempt_duration_ns: u64,
    io_alloc: *IoAlloc,

    pub fn queue_io(self: *const Context, polled: bool, io: linux.io_uring_sqe) error{OutOfIoCapacity}!u64 {
        const entry = self.task_entry;
        if (entry.num_pending_io + entry.num_finished_io == MAX_IO_PER_TASK) {
            return error.OutOfCapacity;
        }

        var sqe = io;
        const io_id = self.io.insert(self.task_id);
        sqe.user_data = io_id;

        if (polled) {
            self.polled_io_queue.push(sqe) catch unreachable;
        } else {
            self.io_queue.push(sqe) catch unreachable;
        }

        return io_id;
    }

    pub fn remove_io_result(self: *const Context, io_id: u64) ?linux.io_uring_cqe {
        for (self.task_entry.finished_io[0..self.task_entry.num_finished_io], 0..) |cqe, idx| {
            if (cqe.user_data == io_id) {
                self.task_entry.num_finished_io -= 1;
                self.task_entry.finished_io[idx] = self.task_entry.finished_io[self.task_entry.num_finished_io];
                return cqe;
            }
        }

        return null;
    }
};

pub fn PollResult(comptime T: type) type {
    return union(enum) {
        ready: T,
        pending,
    };
}

pub const Task = struct {
    ptr: *anyopaque,
    poll_fn: *const fn (*anyopaque, ctx: *Context) PollResult(void),

    pub fn poll(self: Task, ctx: *Context) PollResult(void) {
        return self.poll_fn(self.ptr, ctx);
    }
};
