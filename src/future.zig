const std = @import("std");
const Instant = std.time.Instant;

const Slab = @import("./slab.zig").Slab;

pub const Future = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
};

pub const Context = struct {
    start_t: Instant,
    task_id: u32,
    tasks: *Slab,
    io_result: xd,
};

pub const VTable = struct {
    poll: *const fn (*anyopaque, ctx: *Context) void,
};
