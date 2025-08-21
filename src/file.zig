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
        return .{
            .fd = self.fd,
            .io_ids = undefined,
            .io_running = undefined,
            .io_offset = undefined,
            .io_size = undefined,
            .offset = offset,
            .size = size,
        };
    }
};

fn align_forward(addr: usize, alignment: usize) usize {
    return (addr +% alignment -% 1) & ~(alignment -% 1);
}

fn align_backward(addr: usize, alignment: usize) usize {
    return addr & ~(alignment - 1);
}

pub const DioRead = struct {
    const CONCURRENCY = 4;

    fd: linux.fd_t,

    // Record current pending io state
    io_id: [CONCURRENCY]u64,
    io_is_running: [CONCURRENCY]bool,
    // For retrying if a single op fails
    // io_offset: [CONCURRENCY]u64,
    // io_size: [CONCURRENCY]u32,
    // io_is_retried: [CONCURRENCY]bool,

    io_buf: ?DioBuf,

    // These are modified as IO is issued
    offset: u64,
    size: u32,

    fn init(fd: linux.fd_t, offset: u64, size: u32) DioRead {
        return .{
            .fd = fd,
            .offset = offset,
            .size = size,
            .io_id = undefined,
            .io_is_running = std.mem.zeroes([CONCURRENCY]bool),
            .io_buf = null,
        };
    }

    pub fn poll(self: *DioRead, ctx: Context) PollResult(!DioBuf) {
        const io_buf: DioBuf = if (self.io_buf) |iob| iob else setup_io_buf: {
            if (self.size == 0) {
                return .{ .ready = DioBuf{
                    .alloc = ctx.io_alloc,
                    .alloc_buf = &.{},
                    .data_start = 0,
                    .data_offset = 0,
                } };
            }

            const read_offset = align_backward(self.offset, IoAlloc.ALIGN);
            const read_size = align_forward(self.size, IoAlloc.ALIGN) + (self.offset - read_offset);
            const buf = DioBuf{
                .alloc_buf = try ctx.io_alloc.alloc(read_size),
                .alloc = ctx.io_alloc,
                .data_start = self.offset - read_offset,
                .data_end = (self.offset - read_offset) + self.size,
            };
            self.io_buf = buf;

            self.offset = read_offset;
            self.size = read_size;

            break :setup_io_buf buf;
        };

        // check if any io is finished and count pending io
        var num_pending: u8 = 0;
        for (0..CONCURRENCY) |io_idx| {
            if (self.io_is_running[io_idx]) {
                if (ctx.remove_io_result(self.io_id[io_idx])) |cqe| {
                    switch (cqe.err()) {
                        .SUCCESS => {
                            self.io_running[io_idx] = false;
                        },
                        // TODO: retry
                        else => |e| return e,
                    }
                } else {
                    num_pending += 1;
                }
            }
        }

        if (num_pending == 0 and self.size == 0) {
            return .{ .ready = io_buf };
        }

        // queue new io
        for (0..CONCURRENCY) |io_idx| {
            if (self.size == 0) {
                break;
            }

            if (!self.io_is_running[io_idx]) {
                const io_buf_ptr: [*]u8 = @ptrFromInt(@intFromPtr(io_buf.alloc_buf.ptr) + io_buf.alloc_buf.len - self.size);

                const io_offset = self.offset;
                // do a 512KB single read at max
                const io_size = @min(1 << 19, self.size);
                self.offset += io_size;
                self.size -= io_size;

                // Doesn't have to be mutable but stdlib API for prep_read_fixed is wrong
                var iovec = std.posix.iovec{
                    .base = io_buf_ptr,
                    .len = io_size,
                };

                var sqe = std.mem.zeroes(linux.io_uring_sqe);
                sqe.prep_read_fixed(&sqe, self.fd, &iovec, io_offset, 0);
                self.io_id = try ctx.queue_io(true, sqe);
            }
        }

        return .pending;
    }
};

pub const DioBuf = struct {
    alloc_buf: []align(IoAlloc.ALIGN) u8,
    alloc: *IoAlloc,

    /// Requested data
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

    pub fn poll(self: *Mkdir, ctx: Context) PollResult(!void) {
        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                switch (cqe.err()) {
                    .SUCCESS => {
                        std.debug.assert(cqe.res == 0);
                        return;
                    },
                    else => |e| return e,
                }
            } else {
                return .pending;
            }
        } else {
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            sqe.prep_mkdirat(&sqe, linux.AT.FDCWD, self.path, self.mode);
            self.io_id = try ctx.queue_io(false, sqe);
            return .pending;
        }
    }
};

pub const UnlinkAt = struct {
    path: [:0]const u8,
    flags: u32,

    io_id: ?u64,

    pub fn poll(self: *UnlinkAt, ctx: Context) PollResult(!void) {
        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                switch (cqe.err()) {
                    .SUCCESS => {
                        std.debug.assert(cqe.res == 0);
                        return;
                    },
                    else => |e| return e,
                }
            } else {
                return .pending;
            }
        } else {
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            sqe.prep_unlinkat(&sqe, linux.AT.FDCWD, self.path, self.flags);
            self.io_id = try ctx.queue_io(false, sqe);
            return .pending;
        }
    }
};

pub const Write = struct {
    fd: linux.fd_t,
    offset: u64,
    buf: []const u8,

    io_id: ?u64,

    retried: bool,

    fn init(fd: linux.fd_t, offset: u64, buf: []const u8) Write {
        return .{
            .fd = fd,
            .offset = offset,
            .buf = buf,
            .io_id = null,
            .retried = false,
        };
    }

    fn queue_write(self: *Write, ctx: Context) !void {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        sqe.prep_write(&sqe, self.fd, self.buf, self.offset);
        self.io_id = try ctx.queue_io(false, sqe);
    }

    pub fn poll(self: *Write, ctx: Context) PollResult(!void) {
        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                switch (cqe.err()) {
                    .SUCCESS => {
                        const n_written: usize = @intCast(cqe.res);
                        if (n_written < self.buf.len) {
                        }
                        return;
                    },
                    .EINT => {

                    },
                    else => |e| return e,
                }
            } else {
                return .pending;
            }
        } else {
            try self.queue_write();
            return .pending;
        }
    }
};

pub const Read = struct {
    fd: linux.fd_t,
    offset: u64,
    buf: []u8,

    io_id: ?u64,

    retried: bool,

    fn init(fd: linux.fd_t, offset: u64, buf: []u8) Read {
        return .{
            .fd = fd,
            .offset = offset,
            .buf = buf,
            .io_id = null,
            .retried = false,
        };
    }

    fn queue_read(self: *Read, ctx: Context) !void {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        sqe.prep_read(&sqe, self.fd, self.buf, self.offset);
        self.io_id = try ctx.queue_io(false, sqe);
    }

    pub fn poll(self: *Read, ctx: Context) PollResult(!void) {
        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                switch (cqe.err()) {
                    .SUCCESS => {
                        const n_read: usize = @intCast(cqe.res);
                        if (n_read < self.buf.len) {
                            if (self.retried) {
                                return error.ShortRead;
                            }
                            self.retried = true;
                            try self.queue_read();
                            return .pending;
                        }
                        return;
                    },
                    .EINTR => {
                        if (self.retried) {
                            return error.ShortRead;
                        }
                        self.retried = true;
                        self.queue_read(); 
                        return .pending;
                    },
                    else => |e| return e,
                }
            } else {
                return .pending;
            }
        } else {
            try self.queue_read();
            return .pending;
        }
    }
};

pub const Open = struct {
    path: [:0]const u8,
    flags: i32,
    mode: linux.mode_t,

    io_id: ?u64,

    pub fn poll(self: *Open, ctx: Context) PollResult(!File) {
        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                switch (cqe.err()) {
                    .SUCCESS => return File{ .fd = cqe.res },
                    else => |e| return e,
                }
            } else {
                return .pending;
            }
        } else {
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            sqe.prep_openat(&sqe, linux.AT.FDCWD, self.path, self.flags, self.mode);
            self.io_id = try ctx.queue_io(false, sqe);
            return .pending;
        }
    }
};

pub const Close = struct {
    fd: linux.fd_t,

    io_id: ?u64,

    pub fn poll(self: *Close, ctx: Context) PollResult(!void) {
        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                switch (cqe.err()) {
                    .SUCCESS => return,
                    else => |e| return e,
                }
            } else {
                return .pending;
            }
        } else {
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            sqe.prep_close(self.fd);
            self.io_id = try ctx.queue_io(false, sqe);
            return .pending;
        }
    }
};
