const std = @import("std");
const linux = std.os.linux;

const task_mod = @import("./task.zig");
const Context = task_mod.Context;
const PollResult = task_mod.PollResult;
const IoAlloc = @import("io_alloc.zig").IoAlloc;

/// Allocate a buffer suitable for doing direct_io
///
/// Intended to be used for `DioFile.write`
///
/// Caller should release the memory by calling `DioBuf.free` after their use is done.
pub fn alloc_dio_buffer(ctx: Context, size: u32) DioBuf {
    return DioBuf{
        .alloc_buf = ctx.io_alloc.alloc(size) catch unreachable,
        .alloc = ctx.io_alloc,
        .data_start = 0,
        .data_end = size,
    };
}

pub fn align_forward(addr: usize, alignment: usize) usize {
    return (addr +% alignment -% 1) & ~(alignment -% 1);
}

pub fn align_backward(addr: usize, alignment: usize) usize {
    return addr & ~(alignment - 1);
}

pub const DioWrite = union(enum) {
    pub const CONCURRENCY = 4;
    pub const MAX_IO_SIZE = 1 << 19;

    const Self = @This();

    const Error = linux.E || error{ShortWrite};

    start: struct {
        fd: linux.fd_t,
        offset: u64,
        buf: DioBuf,
    },
    write: struct {
        fd: linux.fd_t,
        file_offset: u64,
        // offset into io_buf, this is modified as write ops are started
        offset: u32,
        io_buf: DioBuf,
        io_id: [CONCURRENCY]u64,
        io_size: [CONCURRENCY]u16,
        io_is_running: [CONCURRENCY]bool,
    },
    fail: struct {
        e: Error,
        io_id: [CONCURRENCY]u64,
        io_is_running: [CONCURRENCY]bool,
    },
    finished: void,

    pub fn init(fd: linux.fd_t, buf: DioBuf, offset: u64) Self {
        std.debug.assert(buf.data_start % IoAlloc.ALIGN == 0);
        std.debug.assert(buf.data_end % IoAlloc.ALIGN == 0);

        return Self{
            .start = .{
                .fd = fd,
                .buf = buf,
                .offset = offset,
            },
        };
    }

    pub fn poll(self: *Self, ctx: Context) PollResult(Error!void) {
        poll: while (true) {
            switch (self.*) {
                .start => |*s| {
                    if (s.buf.size() == 0) {
                        self.* = .finished;
                        return .{ .ready = {} };
                    }

                    var io_is_running: [CONCURRENCY]bool = undefined;
                    for (0..CONCURRENCY) |io_idx| {
                        io_is_running[io_idx] = false;
                    }

                    self.* = .{
                        .write = .{
                            .fd = s.fd,
                            .offset = 0,
                            .io_buf = s.buf,
                            .io_id = undefined,
                            .io_size = undefined,
                            .io_is_running = io_is_running,
                            .file_offset = s.offset,
                        },
                    };
                },
                .write => |*s| {
                    // check if any io is finished and count pending io
                    var num_pending: u8 = 0;
                    for (0..CONCURRENCY) |io_idx| {
                        if (s.io_is_running[io_idx]) {
                            if (ctx.remove_io_result(s.io_id[io_idx])) |cqe| {
                                s.io_is_running[io_idx] = false;
                                switch (cqe.err()) {
                                    .SUCCESS => {
                                        const n_wrote: u32 = @intCast(cqe.res);
                                        std.debug.assert(n_wrote <= s.io_size[io_idx]);
                                        if (n_wrote < s.io_size[io_idx]) {
                                            self.* = .{ .fail = .{ .io_is_running = s.io_is_running, .io_id = s.io_id, .e = error.ShortWrite } };
                                            continue :poll;
                                        }
                                    },
                                    else => |e| {
                                        self.* = .{ .fail = .{ .io_is_running = s.io_is_running, .io_id = s.io_id, .e = e } };
                                        continue :poll;
                                    },
                                }
                            } else {
                                num_pending += 1;
                            }
                        }
                    }

                    if (num_pending == 0 and s.io_buf.size() == s.offset) {
                        self.* = .finished;
                        return .{ .ready = .{} };
                    }

                    // queue new io
                    for (0..CONCURRENCY) |io_idx| {
                        if (s.io_buf.size() == s.offset) {
                            break;
                        }

                        if (!s.io_is_running[io_idx]) {
                            const io_buf_ptr: [*]u8 = s.io_buf.data()[s.offset..].ptr;

                            const io_offset: u64 = s.file_offset + s.offset;

                            const io_size = @min(MAX_IO_SIZE, s.io_buf.size() - s.offset);
                            s.offset += io_size;

                            // Doesn't have to be mutable but stdlib API for prep_write_fixed is wrong
                            var iovec = std.posix.iovec{
                                .base = io_buf_ptr,
                                .len = io_size,
                            };

                            var sqe = std.mem.zeroes(linux.io_uring_sqe);
                            sqe.prep_write_fixed(&sqe, s.fd, &iovec, io_offset, 0);
                            s.io_id = ctx.queue_io(true, sqe);
                            s.io_size[io_idx] = io_size;
                            s.io_is_running[io_idx] = true;
                        }
                    }

                    return .pending;
                },
                .fail => |*s| {
                    // check if any io is finished and count pending io
                    var num_pending: u8 = 0;
                    for (0..CONCURRENCY) |io_idx| {
                        if (s.io_is_running[io_idx]) {
                            if (ctx.remove_io_result(s.io_id[io_idx])) |_| {
                                s.io_is_running[io_idx] = false;
                            } else {
                                num_pending += 1;
                            }
                        }
                    }

                    if (num_pending == 0) {
                        self.* = .finished;
                        return .{ .ready = s.e };
                    } else {
                        return .pending;
                    }
                },
                .finished => unreachable,
            }
        }
    }
};

pub const DioRead = union(enum) {
    const CONCURRENCY = 4;
    const MAX_IO_SIZE = 1 << 19;

    const Self = @This();

    const Error = linux.E || error{ShortRead};

    start: struct {
        fd: linux.fd_t,
        offset: u64,
        size: u32,
    },
    read: struct {
        fd: linux.fd_t,
        offset: u64,
        size: u32,
        io_buf: DioBuf,
        io_id: [CONCURRENCY]u64,
        io_size: [CONCURRENCY]u16,
        io_is_running: [CONCURRENCY]bool,
    },
    fail: struct {
        e: Error,
        io_id: [CONCURRENCY]u64,
        io_is_running: [CONCURRENCY]bool,
    },
    finished: void,

    pub fn init(fd: linux.fd_t, offset: u64, size: u32) Self {
        return Self{
            .start = .{
                .fd = fd,
                .offset = offset,
                .size = size,
            },
        };
    }

    pub fn poll(self: *Self, ctx: Context) PollResult(Error!DioBuf) {
        poll: while (true) {
            switch (self.*) {
                .start => |*s| {
                    if (s.size == 0) {
                        self.* = .finished;
                        return .{
                            .ready = DioBuf{
                                .alloc_buf = &.{},
                                .alloc = ctx.io_alloc,
                                .data_start = 0,
                                .data_end = 0,
                            },
                        };
                    }

                    const read_offset = align_backward(s.offset, IoAlloc.ALIGN);
                    const read_size = align_forward(s.size, IoAlloc.ALIGN) + (s.offset - read_offset);
                    const buf = DioBuf{
                        .alloc_buf = ctx.io_alloc.alloc(read_size) catch unreachable,
                        .alloc = ctx.io_alloc,
                        .data_start = s.offset - read_offset,
                        .data_end = (s.offset - read_offset) + s.size,
                    };

                    var io_is_running: [CONCURRENCY]bool = undefined;
                    for (0..CONCURRENCY) |io_idx| {
                        io_is_running[io_idx] = false;
                    }

                    self.* = .{
                        .read = .{
                            .fd = s.fd,
                            .offset = s.offset,
                            .size = s.size,
                            .io_buf = buf,
                            .io_id = undefined,
                            .io_size = undefined,
                            .io_is_running = io_is_running,
                        },
                    };
                },
                .read => |*s| {
                    // check if any io is finished and count pending io
                    var num_pending: u8 = 0;
                    for (0..CONCURRENCY) |io_idx| {
                        if (s.io_is_running[io_idx]) {
                            if (ctx.remove_io_result(s.io_id[io_idx])) |cqe| {
                                s.io_is_running[io_idx] = false;
                                switch (cqe.err()) {
                                    .SUCCESS => {
                                        const n_read: u32 = @intCast(cqe.res);
                                        std.debug.assert(n_read <= s.io_size[io_idx]);
                                        if (n_read < s.io_size[io_idx]) {
                                            self.* = .{ .fail = .{ .io_is_running = s.io_is_running, .io_id = s.io_id, .e = error.ShortRead } };
                                            continue :poll;
                                        }
                                    },
                                    else => |e| {
                                        self.* = .{ .fail = .{ .io_is_running = s.io_is_running, .io_id = s.io_id, .e = e } };
                                        continue :poll;
                                    },
                                }
                            } else {
                                num_pending += 1;
                            }
                        }
                    }

                    if (num_pending == 0 and s.size == 0) {
                        self.* = .finished;
                        return .{ .ready = s.io_buf };
                    }

                    // queue new io
                    for (0..CONCURRENCY) |io_idx| {
                        if (s.size == 0) {
                            break;
                        }

                        if (!s.io_is_running[io_idx]) {
                            const io_buf_ptr: [*]u8 = @ptrFromInt(@intFromPtr(s.io_buf.alloc_buf.ptr) + s.io_buf.alloc_buf.len - self.size);

                            const io_offset = s.offset;
                            const io_size = @min(MAX_IO_SIZE, s.size);
                            s.offset += io_size;
                            s.size -= io_size;

                            // Doesn't have to be mutable but stdlib API for prep_read_fixed is wrong
                            var iovec = std.posix.iovec{
                                .base = io_buf_ptr,
                                .len = io_size,
                            };

                            var sqe = std.mem.zeroes(linux.io_uring_sqe);
                            sqe.prep_read_fixed(&sqe, s.fd, &iovec, io_offset, 0);
                            s.io_id = ctx.queue_io(true, sqe);
                            s.io_size[io_idx] = io_size;
                            s.io_is_running[io_idx] = true;
                        }
                    }

                    return .pending;
                },
                .fail => |*s| {
                    // check if any io is finished and count pending io
                    var num_pending: u8 = 0;
                    for (0..CONCURRENCY) |io_idx| {
                        if (s.io_is_running[io_idx]) {
                            if (ctx.remove_io_result(s.io_id[io_idx])) |_| {
                                s.io_is_running[io_idx] = false;
                            } else {
                                num_pending += 1;
                            }
                        }
                    }

                    if (num_pending == 0) {
                        self.* = .finished;
                        return .{ .ready = s.e };
                    } else {
                        return .pending;
                    }
                },
                .finished => unreachable,
            }
        }
    }
};

pub const DioBuf = struct {
    alloc_buf: []align(IoAlloc.ALIGN) u8,
    alloc: *IoAlloc,

    // Requested data
    data_start: u32,
    data_end: u32,

    pub fn free(self: *const DioBuf) void {
        self.alloc.free(self.alloc_buf);
    }

    pub fn data(self: *const DioBuf) []u8 {
        return self.alloc_buf[self.data_start..self.data_end];
    }

    pub fn size(self: *const DioBuf) u32 {
        return self.data_end - self.data_start;
    }
};

pub fn mkdir(path: [:0]const u8, mode: linux.mode_t) Mkdir {
    return .{ .path = path, .mode = mode };
}

pub const Mkdir = struct {
    path: [:0]const u8,
    mode: linux.mode_t,

    io_id: ?u64,

    pub fn init(path: [:0]const u8, mode: linux.mode_t) Mkdir {
        return .{
            .path = path,
            .mode = mode,
            .io_id = null,
        };
    }

    pub fn poll(self: *Mkdir, ctx: Context) PollResult(linux.E!void) {
        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                switch (cqe.err()) {
                    .SUCCESS => {
                        std.debug.assert(cqe.res == 0);
                        return;
                    },
                    else => |e| return .{ .ready = e },
                }
            } else {
                return .pending;
            }
        } else {
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            sqe.prep_mkdirat(&sqe, linux.AT.FDCWD, self.path, self.mode);
            self.io_id = ctx.queue_io(false, sqe);
            return .pending;
        }
    }
};

pub const RemoveFile = struct {
    inner: UnlinkAt,

    pub fn init(path: [:0]const u8) RemoveFile {
        return .{
            .inner = UnlinkAt.init(path, 0),
        };
    }

    pub fn poll(self: *RemoveFile, ctx: Context) PollResult(linux.E!void) {
        return self.inner.poll(ctx);
    }
};

pub const RemoveDir = struct {
    inner: UnlinkAt,

    pub fn init(path: [:0]const u8) RemoveDir {
        return .{
            .inner = UnlinkAt.init(path, linux.AT.REMOVEDIR),
        };
    }

    pub fn poll(self: *RemoveDir, ctx: Context) PollResult(linux.E!void) {
        return self.inner.poll(ctx);
    }
};

pub const UnlinkAt = struct {
    path: [:0]const u8,
    flags: u32,

    io_id: ?u64,

    pub fn init(path: [:0]const u8, flags: u32) UnlinkAt {
        return .{
            .path = path,
            .flags = flags,
            .io_id = null,
        };
    }

    pub fn poll(self: *UnlinkAt, ctx: Context) PollResult(linux.E!void) {
        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                switch (cqe.err()) {
                    .SUCCESS => {
                        std.debug.assert(cqe.res == 0);
                        return;
                    },
                    else => |e| return .{ .ready = e },
                }
            } else {
                return .pending;
            }
        } else {
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            sqe.prep_unlinkat(&sqe, linux.AT.FDCWD, self.path, self.flags);
            self.io_id = ctx.queue_io(false, sqe);
            return .pending;
        }
    }
};

pub const Write = struct {
    fd: linux.fd_t,
    offset: u64,
    buf: []const u8,

    io_id: ?u64,

    pub fn init(fd: linux.fd_t, buf: []const u8, offset: u64) Write {
        return .{
            .fd = fd,
            .buf = buf,
            .offset = offset,
            .io_id = null,
        };
    }

    pub fn poll(self: *Write, ctx: Context) PollResult(linux.E!usize) {
        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                switch (cqe.err()) {
                    .SUCCESS => return @intCast(cqe.res),
                    else => |e| return .{ .ready = e },
                }
            } else {
                return .pending;
            }
        } else {
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            sqe.prep_write(&sqe, self.fd, self.buf, self.offset);
            self.io_id = ctx.queue_io(false, sqe);
            return .pending;
        }
    }
};

pub const Read = struct {
    fd: linux.fd_t,
    offset: u64,
    buf: []u8,

    io_id: ?u64,

    pub fn init(fd: linux.fd_t, buf: []u8, offset: u64) Read {
        return .{
            .fd = fd,
            .buf = buf,
            .offset = offset,
            .io_id = null,
        };
    }

    pub fn poll(self: *Read, ctx: Context) PollResult(linux.E!usize) {
        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                switch (cqe.err()) {
                    .SUCCESS => return @intCast(cqe.res),
                    else => |e| return .{ .ready = e },
                }
            } else {
                return .pending;
            }
        } else {
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            sqe.prep_read(&sqe, self.fd, self.buf, self.offset);
            self.io_id = ctx.queue_io(false, sqe);
            return .pending;
        }
    }
};

pub const Open = struct {
    path: [:0]const u8,
    flags: i32,
    mode: linux.mode_t,

    io_id: ?u64,

    pub fn init(path: [:0]const u8, flags: i32, mode: linux.mode_t) Open {
        return .{
            .path = path,
            .flags = flags,
            .mode = mode,
            .io_id = null,
        };
    }

    pub fn poll(self: *Open, ctx: Context) PollResult(linux.E!linux.fd_t) {
        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                switch (cqe.err()) {
                    .SUCCESS => return @intCast(cqe.res),
                    else => |e| return .{ .ready = e },
                }
            } else {
                return .pending;
            }
        } else {
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            sqe.prep_openat(&sqe, linux.AT.FDCWD, self.path, self.flags, self.mode);
            self.io_id = ctx.queue_io(false, sqe);
            return .pending;
        }
    }
};

pub const Close = struct {
    fd: linux.fd_t,

    io_id: ?u64,

    pub fn init(fd: linux.fd_t) Close {
        return .{ .fd = fd };
    }

    pub fn poll(self: *Close, ctx: Context) PollResult(linux.E!void) {
        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                switch (cqe.err()) {
                    .SUCCESS => return,
                    else => |e| return .{ .ready = e },
                }
            } else {
                return .pending;
            }
        } else {
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            sqe.prep_close(self.fd);
            self.io_id = ctx.queue_io(false, sqe);
            return .pending;
        }
    }
};

pub const FAllocate = struct {
    fd: linux.fd_t,
    mode: i32,
    offset: u64,
    size: u64,

    io_id: ?u64,

    pub fn init(fd: linux.fd_t, mode: i32, offset: u64, size: u64) FAllocate {
        return .{
            .fd = fd,
            .mode = mode,
            .offset = offset,
            .size = size,
            .io_id = null,
        };
    }

    pub fn poll(self: *FAllocate, ctx: Context) PollResult(linux.E!void) {
        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                switch (cqe.err()) {
                    .SUCCESS => {
                        std.debug.assert(cqe.res == 0);
                        return .{ .ready = {} };
                    },
                    else => |e| return .{ .ready = e },
                }
            } else {
                return .pending;
            }
        } else {
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            sqe.prep_fallocate(self.fd, self.mode, self.offset, self.size);
            self.io_id = ctx.queue_io(false, sqe);
            return .pending;
        }
    }
};
