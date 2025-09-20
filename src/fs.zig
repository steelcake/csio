const std = @import("std");
const linux = std.os.linux;

const task_mod = @import("./task.zig");
const Context = task_mod.Context;
const Poll = task_mod.Poll;
const Result = task_mod.Result;
const Fd = task_mod.Fd;
const IoOp = task_mod.IoOp;
const DirectIoOp = task_mod.DirectIoOp;

pub fn align_forward(addr: usize, alignment: usize) usize {
    return (addr +% alignment -% 1) & ~(alignment -% 1);
}

pub fn align_backward(addr: usize, alignment: usize) usize {
    return addr & ~(alignment - 1);
}

pub const Mkdir = struct {
    op: IoOp,

    pub fn init(path: [:0]const u8, mode: linux.mode_t) Mkdir {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        sqe.prep_mkdirat(linux.AT.FDCWD, path, mode);

        return .{
            .op = IoOp.init(sqe),
        };
    }

    pub fn poll(self: *Mkdir, ctx: *const Context) Poll(Result(void, linux.E)) {
        switch (self.op.poll(ctx)) {
            .ready => |res| {
                switch (res) {
                    .ok => |r| {
                        std.debug.assert(r == 0);
                        return .{ .ready = .{ .ok = {} } };
                    },
                    .err => |e| {
                        return .{ .ready = .{ .err = e } };
                    },
                }
            },
            .pending => return .pending,
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
    op: IoOp,

    pub fn init(path: [:0]const u8, flags: u32) UnlinkAt {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        sqe.prep_unlinkat(linux.AT.FDCWD, path, flags);

        return .{
            .op = IoOp.init(sqe),
        };
    }

    pub fn poll(self: *UnlinkAt, ctx: *const Context) Poll(Result(void, linux.E)) {
        switch (self.op.poll(ctx)) {
            .ready => |res| {
                switch (res) {
                    .ok => |r| {
                        std.debug.assert(r == 0);
                        return .{ .ready = .{ .ok = {} } };
                    },
                    .err => |e| {
                        return .{ .ready = .{ .err = e } };
                    },
                }
            },
            .pending => return .pending,
        }
    }
};

pub const DirectRead = struct {
    op: DirectIoOp,

    pub fn init(fd: Fd, buf: []align(512) u8, offset: u64) DirectRead {
        // this is because of a problem with stdlib API
        var iovec = std.posix.iovec{
            .base = buf.ptr,
            .len = buf.len,
        };

        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        switch (fd) {
            .fd => |f| {
                sqe.prep_read_fixed(f, &iovec, offset, 0);
            },
            .fixed => |idx| {
                sqe.prep_read_fixed(@intCast(idx), &iovec, offset, 0);
                sqe.flags |= linux.IOSQE_FIXED_FILE;
            },
        }

        return .{
            .op = DirectIoOp.init(sqe),
        };
    }

    pub fn poll(self: *DirectRead, ctx: *const Context) Poll(Result(usize, linux.E)) {
        switch (self.op.poll(ctx)) {
            .ready => |res| {
                switch (res) {
                    .ok => |r| {
                        return .{ .ready = .{ .ok = @intCast(r) } };
                    },
                    .err => |e| {
                        return .{ .ready = .{ .err = e } };
                    },
                }
            },
            .pending => return .pending,
        }
    }
};

pub const DirectWrite = struct {
    op: DirectIoOp,

    pub fn init(fd: Fd, buf: []const u8, offset: u64) DirectWrite {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        // this is because of a problem with stdlib API
        var iovec = std.posix.iovec{
            .base = @constCast(buf.ptr),
            .len = buf.len,
        };
        switch (fd) {
            .fd => |f| {
                sqe.prep_write_fixed(f, &iovec, offset, 0);
            },
            .fixed => |idx| {
                sqe.prep_write_fixed(@intCast(idx), &iovec, offset, 0);
                sqe.flags |= linux.IOSQE_FIXED_FILE;
            },
        }

        return .{
            .op = DirectIoOp.init(sqe),
        };
    }

    pub fn poll(self: *DirectWrite, ctx: *const Context) Poll(Result(usize, linux.E)) {
        switch (self.op.poll(ctx)) {
            .ready => |res| {
                switch (res) {
                    .ok => |r| {
                        return .{ .ready = .{ .ok = @intCast(r) } };
                    },
                    .err => |e| {
                        return .{ .ready = .{ .err = e } };
                    },
                }
            },
            .pending => return .pending,
        }
    }
};

pub const Read = struct {
    op: IoOp,

    pub fn init(fd: Fd, buf: []u8, offset: u64) Read {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        switch (fd) {
            .fd => |f| {
                sqe.prep_read(f, buf, offset);
            },
            .fixed => |idx| {
                sqe.prep_read(@intCast(idx), buf, offset);
                sqe.flags |= linux.IOSQE_FIXED_FILE;
            },
        }

        return .{
            .op = IoOp.init(sqe),
        };
    }

    pub fn poll(self: *Read, ctx: *const Context) Poll(Result(usize, linux.E)) {
        switch (self.op.poll(ctx)) {
            .ready => |res| {
                switch (res) {
                    .ok => |r| {
                        return .{ .ready = .{ .ok = @intCast(r) } };
                    },
                    .err => |e| {
                        return .{ .ready = .{ .err = e } };
                    },
                }
            },
            .pending => return .pending,
        }
    }
};

pub const Write = struct {
    op: IoOp,

    pub fn init(fd: Fd, buf: []const u8, offset: u64) Write {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        switch (fd) {
            .fd => |f| {
                sqe.prep_write(f, buf, offset);
            },
            .fixed => |idx| {
                sqe.prep_write(@intCast(idx), buf, offset);
                sqe.flags |= linux.IOSQE_FIXED_FILE;
            },
        }

        return .{
            .op = IoOp.init(sqe),
        };
    }

    pub fn poll(self: *Write, ctx: *const Context) Poll(Result(usize, linux.E)) {
        switch (self.op.poll(ctx)) {
            .ready => |res| {
                switch (res) {
                    .ok => |r| {
                        return .{ .ready = .{ .ok = @intCast(r) } };
                    },
                    .err => |e| {
                        return .{ .ready = .{ .err = e } };
                    },
                }
            },
            .pending => return .pending,
        }
    }
};

pub const Open = struct {
    op: IoOp,

    pub fn init(path: [:0]const u8, flags: linux.O, mode: linux.mode_t) Open {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        sqe.prep_openat(linux.AT.FDCWD, path, flags, mode);

        return .{
            .op = IoOp.init(sqe),
        };
    }

    pub fn poll(self: *Open, ctx: *const Context) Poll(Result(linux.fd_t, linux.E)) {
        switch (self.op.poll(ctx)) {
            .ready => |res| {
                switch (res) {
                    .ok => |r| {
                        return .{ .ready = .{ .ok = @intCast(r) } };
                    },
                    .err => |e| {
                        return .{ .ready = .{ .err = e } };
                    },
                }
            },
            .pending => return .pending,
        }
    }
};

pub const Close = struct {
    op: IoOp,

    pub fn init(fd: linux.fd_t) Close {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        sqe.prep_close(fd);
        return .{
            .op = IoOp.init(sqe),
        };
    }

    pub fn poll(self: *Close, ctx: *const Context) Poll(Result(void, linux.E)) {
        switch (self.op.poll(ctx)) {
            .ready => |res| {
                switch (res) {
                    .ok => |r| {
                        std.debug.assert(r == 0);
                        return .{ .ready = .{ .ok = {} } };
                    },
                    .err => |e| {
                        return .{ .ready = .{ .err = e } };
                    },
                }
            },
            .pending => return .pending,
        }
    }
};

pub const FAllocate = struct {
    op: IoOp,

    pub fn init(fd: Fd, mode: i32, offset: u64, size: u64) FAllocate {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        switch (fd) {
            .fd => |f| {
                sqe.prep_fallocate(f, mode, offset, size);
            },
            .fixed => |idx| {
                sqe.prep_fallocate(@intCast(idx), mode, offset, size);
                sqe.flags |= linux.IOSQE_FIXED_FILE;
            },
        }

        return .{
            .op = IoOp.init(sqe),
        };
    }

    pub fn poll(self: *FAllocate, ctx: *const Context) Poll(Result(void, linux.E)) {
        switch (self.op.poll(ctx)) {
            .ready => |res| {
                switch (res) {
                    .ok => |r| {
                        std.debug.assert(r == 0);
                        return .{ .ready = .{ .ok = {} } };
                    },
                    .err => |e| {
                        return .{ .ready = .{ .err = e } };
                    },
                }
            },
            .pending => return .pending,
        }
    }
};
