const std = @import("std");
const linux = std.os.linux;

const task_mod = @import("./task.zig");
const Context = task_mod.Context;
const Poll = task_mod.Poll;
const Result = task_mod.Result;
const Fd = task_mod.Fd;

pub fn align_forward(addr: usize, alignment: usize) usize {
    return (addr +% alignment -% 1) & ~(alignment -% 1);
}

pub fn align_backward(addr: usize, alignment: usize) usize {
    return addr & ~(alignment - 1);
}

pub const Mkdir = struct {
    path: [:0]const u8,
    mode: linux.mode_t,

    io_id: ?u64,
    finished: bool,

    pub fn init(path: [:0]const u8, mode: linux.mode_t) Mkdir {
        return .{
            .path = path,
            .mode = mode,
            .io_id = null,
            .finished = false,
        };
    }

    pub fn poll(self: *Mkdir, ctx: *const Context) Poll(Result(void, linux.E)) {
        std.debug.assert(!self.finished);

        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                self.finished = true;
                switch (cqe.err()) {
                    .SUCCESS => {
                        std.debug.assert(cqe.res == 0);
                        return .{ .ready = .{ .ok = {} } };
                    },
                    else => |e| return .{ .ready = .{ .err = e } },
                }
            } else {
                return .pending;
            }
        } else {
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            sqe.prep_mkdirat(linux.AT.FDCWD, self.path, self.mode);
            self.io_id = ctx.queue_io(sqe);
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

    pub fn poll(self: *RemoveFile, ctx: *const Context) Poll(Result(void, linux.E)) {
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

    pub fn poll(self: *RemoveDir, ctx: *const Context) Poll(Result(void, linux.E)) {
        return self.inner.poll(ctx);
    }
};

pub const UnlinkAt = struct {
    path: [:0]const u8,
    flags: u32,

    io_id: ?u64,
    finished: bool,

    pub fn init(path: [:0]const u8, flags: u32) UnlinkAt {
        return .{
            .path = path,
            .flags = flags,
            .io_id = null,
            .finished = false,
        };
    }

    pub fn poll(self: *UnlinkAt, ctx: *const Context) Poll(Result(void, linux.E)) {
        std.debug.assert(!self.finished);

        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                self.finished = true;
                switch (cqe.err()) {
                    .SUCCESS => {
                        std.debug.assert(cqe.res == 0);
                        return .{ .ready = .{ .ok = {} } };
                    },
                    else => |e| return .{ .ready = .{ .err = e } },
                }
            } else {
                return .pending;
            }
        } else {
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            sqe.prep_unlinkat(linux.AT.FDCWD, self.path, self.flags);
            self.io_id = ctx.queue_io(sqe);
            return .pending;
        }
    }
};

pub const DirectWrite = struct {
    inner: Write,

    pub fn init(fd: Fd, buf: []const u8, offset: u64) DirectWrite {
        return .{
            .inner = .{
                .fd = fd,
                .buf = buf,
                .offset = offset,
                .io_id = null,
                .finished = false,
                .direct = true,
            },
        };
    }

    pub fn poll(self: *DirectWrite, ctx: *const Context) Poll(Result(usize, linux.E)) {
        return self.inner.poll(ctx);
    }
};

pub const Write = struct {
    fd: Fd,
    offset: u64,
    buf: []const u8,
    direct: bool,

    io_id: ?u64,
    finished: bool,

    pub fn init(fd: Fd, buf: []const u8, offset: u64) Write {
        return .{
            .fd = fd,
            .buf = buf,
            .offset = offset,
            .io_id = null,
            .finished = false,
            .direct = false,
        };
    }

    pub fn poll(self: *Write, ctx: *const Context) Poll(Result(usize, linux.E)) {
        std.debug.assert(!self.finished);

        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                self.finished = true;
                switch (cqe.err()) {
                    .SUCCESS => return .{ .ready = .{ .ok = @intCast(cqe.res) } },
                    else => |e| return .{ .ready = .{ .err = e } },
                }
            } else {
                return .pending;
            }
        } else {
            if (self.buf.len == 0) {
                self.finished = true;
                return .{ .ready = .{ .ok = 0 } };
            }
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            // this is because of a problem with stdlib API
            var iovec = std.posix.iovec{
                .base = @constCast(self.buf.ptr),
                .len = self.buf.len,
            };
            switch (self.fd) {
                .fd => |fd| {
                    sqe.prep_write_fixed(fd, &iovec, self.offset, 0);
                },
                .fixed => |idx| {
                    sqe.prep_write_fixed(@intCast(idx), &iovec, self.offset, 0);
                    sqe.flags |= linux.IOSQE_FIXED_FILE;
                },
            }
            self.io_id = if (self.direct) ctx.queue_polled_io(sqe) else ctx.queue_io(sqe);
            return .pending;
        }
    }
};

pub const DirectRead = struct {
    inner: Read,

    pub fn init(fd: Fd, buf: []u8, offset: u64) DirectRead {
        return .{
            .inner = .{
                .fd = fd,
                .buf = buf,
                .offset = offset,
                .io_id = null,
                .finished = false,
                .direct = true,
            },
        };
    }

    pub fn poll(self: *DirectRead, ctx: *const Context) Poll(Result(usize, linux.E)) {
        return self.inner.poll(ctx);
    }
};

pub const Read = struct {
    fd: Fd,
    offset: u64,
    buf: []u8,
    direct: bool,

    io_id: ?u64,
    finished: bool,

    pub fn init(fd: Fd, buf: []u8, offset: u64) Read {
        return .{
            .fd = fd,
            .buf = buf,
            .offset = offset,
            .io_id = null,
            .finished = false,
            .direct = false,
        };
    }

    pub fn poll(self: *Read, ctx: *const Context) Poll(Result(usize, linux.E)) {
        std.debug.assert(!self.finished);

        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                self.finished = true;
                switch (cqe.err()) {
                    .SUCCESS => return .{ .ready = .{ .ok = @intCast(cqe.res) } },
                    else => |e| return .{ .ready = .{ .err = e } },
                }
            } else {
                return .pending;
            }
        } else {
            if (self.buf.len == 0) {
                self.finished = true;
                return .{ .ready = .{ .ok = 0 } };
            }
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            // this is because of a problem with stdlib API
            var iovec = std.posix.iovec{
                .base = self.buf.ptr,
                .len = self.buf.len,
            };
            switch (self.fd) {
                .fd => |fd| {
                    sqe.prep_read_fixed(fd, &iovec, self.offset, 0);
                },
                .fixed => |idx| {
                    sqe.prep_read_fixed(@intCast(idx), &iovec, self.offset, 0);
                    sqe.flags |= linux.IOSQE_FIXED_FILE;
                },
            }
            self.io_id = if (self.direct) ctx.queue_polled_io(sqe) else ctx.queue_io(sqe);
            return .pending;
        }
    }
};

pub const Open = struct {
    path: [:0]const u8,
    flags: linux.O,
    mode: linux.mode_t,

    io_id: ?u64,
    finished: bool,

    pub fn init(path: [:0]const u8, flags: linux.O, mode: linux.mode_t) Open {
        return .{
            .path = path,
            .flags = flags,
            .mode = mode,
            .io_id = null,
            .finished = false,
        };
    }

    pub fn poll(self: *Open, ctx: *const Context) Poll(Result(linux.fd_t, linux.E)) {
        std.debug.assert(!self.finished);

        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                self.finished = true;

                switch (cqe.err()) {
                    .SUCCESS => return .{ .ready = .{ .ok = @intCast(cqe.res) } },
                    else => |e| return .{ .ready = .{ .err = e } },
                }
            } else {
                return .pending;
            }
        } else {
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            sqe.prep_openat(linux.AT.FDCWD, self.path, self.flags, self.mode);
            self.io_id = ctx.queue_io(sqe);
            return .pending;
        }
    }
};

pub const Close = struct {
    fd: linux.fd_t,

    io_id: ?u64,
    finished: bool,

    pub fn init(fd: linux.fd_t) Close {
        return .{ .fd = fd, .io_id = null, .finished = false };
    }

    pub fn poll(self: *Close, ctx: *const Context) Poll(Result(void, linux.E)) {
        std.debug.assert(!self.finished);

        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                self.finished = true;
                switch (cqe.err()) {
                    .SUCCESS => return .{ .ready = .{ .ok = {} } },
                    else => |e| return .{ .ready = .{ .err = e } },
                }
            } else {
                return .pending;
            }
        } else {
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            sqe.prep_close(self.fd);
            self.io_id = ctx.queue_io(sqe);
            return .pending;
        }
    }
};

pub const FAllocate = struct {
    fd: Fd,
    mode: i32,
    offset: u64,
    size: u64,

    io_id: ?u64,
    finished: bool,

    pub fn init(fd: Fd, mode: i32, offset: u64, size: u64) FAllocate {
        return .{
            .fd = fd,
            .mode = mode,
            .offset = offset,
            .size = size,
            .io_id = null,
            .finished = false,
        };
    }

    pub fn poll(self: *FAllocate, ctx: *const Context) Poll(Result(void, linux.E)) {
        std.debug.assert(!self.finished);

        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                self.finished = true;
                switch (cqe.err()) {
                    .SUCCESS => {
                        std.debug.assert(cqe.res == 0);
                        return .{ .ready = .{ .ok = {} } };
                    },
                    else => |e| return .{ .ready = .{ .err = e } },
                }
            } else {
                return .pending;
            }
        } else {
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            switch (self.fd) {
                .fd => |fd| {
                    sqe.prep_fallocate(fd, self.mode, self.offset, self.size);
                },
                .fixed => |idx| {
                    sqe.prep_fallocate(@intCast(idx), self.mode, self.offset, self.size);
                    sqe.flags |= linux.IOSQE_FIXED_FILE;
                },
            }
            self.io_id = ctx.queue_io(sqe);
            return .pending;
        }
    }
};
