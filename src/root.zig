const slab = @import("./slab.zig");
const slice_map = @import("./slice_map.zig");
const queue = @import("./queue.zig");
const task = @import("./task.zig");
const executor = @import("./executor.zig");
const io_alloc = @import("./io_alloc.zig");

pub const fs = @import("./fs.zig");
pub const net = @import("./net.zig");
pub const Executor = executor.Executor;
pub const Task = task.Task;
pub const Context = task.Context;
pub const Poll = task.Poll;
pub const Result = task.Result;
pub const Fd = task.Fd;
pub const MAX_IO_PER_TASK = task.MAX_IO_PER_TASK;
pub const IO_ALIGN = io_alloc.IoAlloc.ALIGN;

test {
    _ = io_alloc;
    _ = task;
    _ = executor;
    _ = slab;
    _ = slice_map;
    _ = fs;
    _ = net;
    _ = queue;
}
