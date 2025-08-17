const std = @import("std");
const linux = std.os.linux;

const task_mod = @import("./task.zig");
const Context = task_mod.Context;
const PollResult = task_mod.PollResult;

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
        };
    }

    pub fn write(self: *const File, buf: []const u8, offset: u64) Write {
        return .{
            .fd = self.fd,
            .offset = offset,
            .buf = buf,
        };
    }

    pub fn unlink(path: [:0]const u8, flags: u32) Unlink {
        return .{ .path = path, .flags = flags };
    }

    pub fn mkdir(path: [:0]const u8, mode: linux.mode_t) Mkdir {
        return .{ .path = path, .mode = mode };
    }
};

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

pub const Unlink = struct {
    path: [:0]const u8,
    flags: u32,

    io_id: ?u64,

    pub fn poll(self: *Unlink, ctx: Context) PollResult(!void) {
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

    pub fn poll(self: *Write, ctx: Context) PollResult(!usize) {
        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                switch (cqe.err()) {
                    .SUCCESS => return @intCast(cqe.res),
                    else => |e| return e,
                }
            } else {
                return .pending;
            }
        } else {
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            sqe.prep_write(&sqe, self.fd, self.buf, self.offset);
            self.io_id = try ctx.queue_io(false, sqe);
            return .pending;
        }
    }
};

pub const Read = struct {
    fd: linux.fd_t,
    offset: u64,
    buf: []u8,

    io_id: ?u64,

    pub fn poll(self: *Read, ctx: Context) PollResult(!usize) {
        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                switch (cqe.err()) {
                    .SUCCESS => return @intCast(cqe.res),
                    else => |e| return e,
                }
            } else {
                return .pending;
            }
        } else {
            var sqe = std.mem.zeroes(linux.io_uring_sqe);
            sqe.prep_read(&sqe, self.fd, self.buf, self.offset);
            self.io_id = try ctx.queue_io(false, sqe);
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
