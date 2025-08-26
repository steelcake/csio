const std = @import("std");
const linux = std.os.linux;

const task_mod = @import("./task.zig");
const Context = task_mod.Context;
const PollResult = task_mod.PollResult;
const IoAlloc = @import("io_alloc.zig").IoAlloc;

pub const DioFile = struct {
    fd: linux.fd_t,

    pub fn open(path: [:0]const u8, flags: i32, mode: linux.mode_t) Open {
        return .{
            .path = path,
            .flags = flags,
            .mode = mode,
            .io_id = null,
        };
    }

    pub fn close(self: DioFile) Close {
        return .{
            .fd = self.fd,
            .io_id = null,
        };
    }

    pub fn read(self: *const DioFile, offset: u64, size: u32) DioRead {
        return DioRead.init(self.fd, offset, size);
    }

    /// This function doesn't take ownership of `buf`. The caller still has to call `.release()` on it.
    pub fn write(self: *const DioFile, buf: *const DioBuf, offset: u64) DioWrite {
        return DioWrite.init(self.fd, buf, offset);
    }
};

pub fn align_forward(addr: usize, alignment: usize) usize {
    return (addr +% alignment -% 1) & ~(alignment -% 1);
}

pub fn align_backward(addr: usize, alignment: usize) usize {
    return addr & ~(alignment - 1);
}

const MAX_DIO_SIZE = 1 << 19; // 512KB
const DIO_CONCURRENCY = 4; // per single read/write call

pub const DioWrite = struct {
    const CONCURRENCY = DIO_CONCURRENCY;

    fd: linux.fd_t,

    // Record concurrent pending io state
    io_id: [CONCURRENCY]u64,
    io_size: [CONCURRENCY]u16,
    io_is_running: [CONCURRENCY]bool,

    io_buf: *const DioBuf,

    // These are modified as IO is issued
    offset: u64,
    size: usize,

    fn init(fd: linux.fd_t, buf: *const DioBuf, offset: u64) DioWrite {
        return .{
            .fd = fd,
            .io_buf = buf,
            .offset = offset,
            .size = @intCast(buf.alloc_buf.len),
            .io_id = undefined,
            .io_size = undefined,
            .io_is_running = std.mem.zeroes([CONCURRENCY]bool),
        };
    }

    pub fn poll(self: *DioWrite, ctx: Context) PollResult(!void) {
        // check if any io is finished and count pending io
        var num_pending: u8 = 0;
        for (0..CONCURRENCY) |io_idx| {
            if (self.io_is_running[io_idx]) {
                if (ctx.remove_io_result(self.io_id[io_idx])) |cqe| {
                    switch (cqe.err()) {
                        .SUCCESS => {
                            const n: u32 = @intCast(cqe.res);
                            std.debug.assert(n <= self.io_size[io_idx]);
                            if (n < self.io_size[io_idx]) {
                                return .{ .ready = error.ShortWrite };
                            }
                            self.io_is_running[io_idx] = false;
                        },
                        else => |e| return .{ .ready = e },
                    }
                } else {
                    num_pending += 1;
                }
            }
        }

        if (num_pending == 0 and self.size == 0) {
            return .{ .ready = {} };
        }

        // queue new io
        for (0..CONCURRENCY) |io_idx| {
            if (self.size == 0) {
                break;
            }

            if (!self.io_is_running[io_idx]) {
                const io_buf_ptr: [*]u8 = @ptrFromInt(@intFromPtr(self.io_buf.alloc_buf.ptr) + self.io_buf.alloc_buf.len - self.size);

                const io_offset = self.offset;
                // do a 512KB single write at max
                const io_size = @min(MAX_DIO_SIZE, self.size);
                self.offset += io_size;
                self.size -= io_size;

                // Doesn't have to be mutable but stdlib API for prep_write_fixed is wrong
                var iovec = std.posix.iovec{
                    .base = io_buf_ptr,
                    .len = io_size,
                };

                var sqe = std.mem.zeroes(linux.io_uring_sqe);
                sqe.prep_write_fixed(&sqe, self.fd, &iovec, io_offset, 0);
                self.io_id = ctx.queue_io(true, sqe);
                self.io_size[io_idx] = io_size;
                self.io_is_running[io_idx] = true;
            }
        }

        return .pending;
    }
};

pub const DioRead = union(enum) {
    const CONCURRENCY = DIO_CONCURRENCY;

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

    fn init(fd: linux.fd_t, offset: u64, size: u32) Self {
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
                        .alloc_buf = try ctx.io_alloc.alloc(read_size),
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
                            // do a 512KB single read at max
                            const io_size = @min(MAX_DIO_SIZE, s.size);
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

    pub fn release(self: DioBuf) void {
        self.alloc.free(self.alloc_buf);
    }

    pub fn data(self: DioBuf) []const u8 {
        return self.alloc_buf[self.data_start..self.data_end];
    }
};

pub const File = struct {
    fd: linux.fd_t,

    pub fn open(path: [:0]const u8, flags: i32, mode: linux.mode_t) Open {
        return .{
            .path = path,
            .flags = flags,
            .mode = mode,
            .io_id = null,
        };
    }

    pub fn close(self: File) Close {
        return .{
            .fd = self.fd,
            .io_id = null,
        };
    }

    pub fn read(self: *const File, buf: []u8, offset: u64) Read {
        return .{
            .fd = self.fd,
            .offset = offset,
            .buf = buf,
            .retried = false,
        };
    }

    pub fn write(self: *const File, buf: []const u8, offset: u64) Write {
        return .{
            .fd = self.fd,
            .offset = offset,
            .buf = buf,
            .retried = false,
        };
    }
};

pub fn unlink(path: [:0]const u8) UnlinkAt {
    return .{ .path = path, .flags = 0 };
}

pub fn rmdir(path: [:0]const u8) UnlinkAt {
    return .{ .path = path, .flags = linux.AT.REMOVEDIR };
}

pub fn mkdir(path: [:0]const u8, mode: linux.mode_t) Mkdir {
    return .{ .path = path, .mode = mode };
}

pub const Mkdir = struct {
    path: [:0]const u8,
    mode: linux.mode_t,

    io_id: ?u64,

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

pub const UnlinkAt = struct {
    path: [:0]const u8,
    flags: u32,

    io_id: ?u64,

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

    pub fn poll(self: *Open, ctx: Context) PollResult(linux.E!File) {
        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                switch (cqe.err()) {
                    .SUCCESS => return File{ .fd = cqe.res },
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
