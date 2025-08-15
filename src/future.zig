const std = @import("std");
const Instant = std.time.Instant;
const linux = std.os.linux;

const slab_mod = @import("./slab.zig");
const Slab = slab_mod.Slab;
const SlabKey = slab_mod.Key;

const SliceMap = @import("./slice_map.zig").SliceMap;
const Queue = @import("./queue.zig").Queue;

pub const Future = struct {
    ptr: *anyopaque,
    poll_fn: *const fn (*anyopaque, ctx: *Context) void,

    pub fn poll(self: Future, ctx: *Context) void {
        return self.poll_fn(self.ptr, ctx);
    }
};

pub const Context = struct {
    start_t: Instant,
    task_id: u32,
    tasks: *Slab(Future),
    io_results: *SliceMap(SlabKey, i32),
    io_queue: *Queue(linux.io_uring_sqe),
    dio_queue: *Queue(linux.io_uring_sqe),
    preempt_duration_ns: u64,
    io: *Slab(SlabKey),
    to_notify: *SliceMap(SlabKey, void),
    notify_when: *SliceMap(),
};
