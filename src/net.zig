const std = @import("std");
const linux = std.os.linux;

const task_mod = @import("./task.zig");
const Context = task_mod.Context;
const Poll = task_mod.Poll;
const Result = task_mod.Result;
const Fd = task_mod.Fd;
const IoOp = task_mod.IoOp;

pub const Socket = struct {
    op: IoOp,

    pub fn init(domain: i32, socket_type: i32, protocol: i32) Socket {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        sqe.prep_socket(domain, socket_type, protocol);
        const op = IoOp.init(sqe);
        return .{
            .op = op,
        };
    }

    pub fn poll(self: *Socket, ctx: *const Context) Poll(Result(linux.fd_t, linux.E)) {
        switch (self.op.poll(ctx)) {
            .ready => |res| {
                std.debug.assert(res >= 0);
                return .{ .ready = .{ .ok = @intCast(res) } };
            },
            .pending => return .pending,
        }
    }
};

pub const Bind = struct {
    op: IoOp,

    pub fn init(fd: Fd, addr: *const linux.sockaddr, addr_len: u32, flags: u32) Bind {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        switch (fd) {
            .fd => |f| {
                sqe.prep_bind(f, addr, addr_len, flags);
            },
            .fixed => |idx| {
                sqe.prep_bind(@intCast(idx), addr, addr_len, flags);
                sqe.flags |= linux.IOSQE_FIXED_FILE;
            },
        }
        return .{
            .op = IoOp.init(sqe),
        };
    }

    pub fn poll(self: *Bind, ctx: *const Context) Poll(Result(void, linux.E)) {
        switch (self.op.poll(ctx)) {
            .ready => |res| {
                std.debug.assert(res == 0);
                return .{ .ready = .{ .ok = {} } };
            },
            .pending => return .pending,
        }
    }
};

pub const Listen = struct {
    op: IoOp,

    pub fn init(fd: Fd, backlog: i32) Listen {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        switch (fd) {
            .fd => |f| {
                sqe.prep_listen(f, @intCast(backlog), 0);
            },
            .fixed => |idx| {
                sqe.prep_listen(@intCast(idx), @intCast(backlog), 0);
                sqe.flags |= linux.IOSQE_FIXED_FILE;
            },
        }

        return .{
            .op = IoOp.init(sqe),
        };
    }

    pub fn poll(self: *Listen, ctx: *const Context) Poll(Result(void, linux.E)) {
        switch (self.op.poll(ctx)) {
            .ready => |res| {
                std.debug.assert(res == 0);
                return .{ .ready = .{ .ok = {} } };
            },
            .pending => return .pending,
        }
    }
};

pub const Connect = struct {
    op: IoOp,

    pub fn init(fd: Fd, addr: *const linux.sockaddr, addr_len: u32) Connect {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        switch (fd) {
            .fd => |f| {
                sqe.prep_connect(f, addr, addr_len);
            },
            .fixed => |idx| {
                sqe.prep_connect(@intCast(idx), addr, addr_len);
                sqe.flags |= linux.IOSQE_FIXED_FILE;
            },
        }

        return .{
            .op = IoOp.init(sqe),
        };
    }

    pub fn poll(self: *Connect, ctx: *const Context) Poll(Result(void, linux.E)) {
        switch (self.op.poll(ctx)) {
            .ready => |res| {
                std.debug.assert(res == 0);
                return .{ .ready = .{ .ok = {} } };
            },
            .pending => return .pending,
        }
    }
};

pub const Recv = struct {
    op: IoOp,

    pub fn init(fd: Fd, buf: []u8, flags: i32) Recv {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        switch (fd) {
            .fd => |f| {
                sqe.prep_recv(f, buf, flags);
            },
            .fixed => |idx| {
                sqe.prep_recv(@intCast(idx), buf, flags);
                sqe.flags |= linux.IOSQE_FIXED_FILE;
            },
        }

        return .{
            .op = IoOp.init(sqe),
        };
    }

    pub fn poll(self: *Recv, ctx: *const Context) Poll(Result(usize, linux.E)) {
        switch (self.op.poll(ctx)) {
            .ready => |res| {
                return .{ .ready = .{ .ok = @intCast(res) } };
            },
            .pending => return .pending,
        }
    }
};

pub const Send = struct {
    op: IoOp,

    pub fn init(fd: Fd, buf: []const u8, flags: i32) Send {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        switch (fd) {
            .fd => |f| {
                sqe.prep_send(f, buf, flags);
            },
            .fixed => |idx| {
                sqe.prep_send(@intCast(idx), buf, flags);
                sqe.flags |= linux.IOSQE_FIXED_FILE;
            },
        }

        return .{
            .op = IoOp.init(sqe),
        };
    }

    pub fn poll(self: *Send, ctx: *const Context) Poll(Result(usize, linux.E)) {
        switch (self.op.poll(ctx)) {
            .ready => |res| {
                return .{ .ready = .{ .ok = @intCast(res) } };
            },
            .pending => return .pending,
        }
    }
};

pub const Accept = struct {
    op: IoOp,

    pub fn init(fd: Fd, addr: ?*const linux.sockaddr, addr_len: ?u32, flags: u32) Accept {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        switch (fd) {
            .fd => |f| {
                sqe.prep_accept(f, addr, addr_len, flags);
            },
            .fixed => |idx| {
                sqe.prep_accept(@intCast(idx), addr, addr_len, flags);
                sqe.flags |= linux.IOSQE_FIXED_FILE;
            },
        }

        return .{
            .op = IoOp.init(sqe),
        };
    }

    pub fn poll(self: *Accept, ctx: *const Context) Poll(Result(linux.fd_t, linux.E)) {
        switch (self.op.poll(ctx)) {
            .ready => |res| {
                std.debug.assert(res >= 0);
                return .{ .ready = .{ .ok = @intCast(res) } };
            },
            .pending => return .pending,
        }
    }
};
