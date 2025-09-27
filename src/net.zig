const std = @import("std");
const linux = std.os.linux;

const task_mod = @import("./task.zig");
const Context = task_mod.Context;
const Poll = task_mod.Poll;
const Result = task_mod.Result;
const Fd = task_mod.Fd;
const IoOp = task_mod.IoOp;

pub const Close = task_mod.Close;

pub const Socket = struct {
    op: IoOp,

    pub fn init(domain: u32, socket_type: u32, protocol: u32) Socket {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        sqe.prep_socket(domain, socket_type, protocol, 0);
        const op = IoOp.init(sqe);
        return .{
            .op = op,
        };
    }

    pub fn poll(self: *Socket, ctx: *const Context) Poll(Result(linux.fd_t, linux.E)) {
        switch (self.op.poll(ctx)) {
            .ready => |res| {
                switch (res) {
                    .ok => |r| {
                        std.debug.assert(r >= 0);
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

// pub const SetSockOpt = struct {
//     op: IoOp,

//     pub fn init(fd: Fd, level: i32, optname: i32, optval: *anytype, ) SetSockOpt {

//     }
// };

pub const Bind = struct {
    op: IoOp,

    pub fn init(fd: Fd, addr: *const linux.sockaddr, addr_len: u32, flags: i32) Bind {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        switch (fd) {
            .fd => |f| {
                sqe.prep_bind(f, addr, addr_len, @bitCast(flags));
            },
            .fixed => |idx| {
                sqe.prep_bind(@intCast(idx), addr, addr_len, @bitCast(flags));
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

pub const Recv = struct {
    op: IoOp,

    pub fn init(fd: Fd, buf: []u8, flags: i32) Recv {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        switch (fd) {
            .fd => |f| {
                sqe.prep_recv(f, buf, @bitCast(flags));
            },
            .fixed => |idx| {
                sqe.prep_recv(@intCast(idx), buf, @bitCast(flags));
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
                switch (res) {
                    .ok => |r| return .{ .ready = .{ .ok = @intCast(r) } },
                    .err => |e| return .{ .ready = .{ .err = e } },
                }
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
                sqe.prep_send(f, buf, @bitCast(flags));
            },
            .fixed => |idx| {
                sqe.prep_send(@intCast(idx), buf, @bitCast(flags));
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
                switch (res) {
                    .ok => |r| return .{ .ready = .{ .ok = @intCast(r) } },
                    .err => |e| return .{ .ready = .{ .err = e } },
                }
            },
            .pending => return .pending,
        }
    }
};

pub const Accept = struct {
    op: IoOp,

    pub fn init(fd: Fd, addr: ?*linux.sockaddr, addr_len: ?*u32, flags: i32) Accept {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        switch (fd) {
            .fd => |f| {
                sqe.prep_accept(f, addr, addr_len, @bitCast(flags));
            },
            .fixed => |idx| {
                sqe.prep_accept(@intCast(idx), addr, addr_len, @bitCast(flags));
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
                switch (res) {
                    .ok => |r| {
                        std.debug.assert(r >= 0);
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

/// implements exact sending/receiving by looping until send/receive op goes through the entire buffer
fn Exact(comptime Op: type) type {
    return struct {
        const Buf = switch (Op) {
            Send => []const u8,
            Recv => []u8,
            else => @compileError("invalid exact op type"),
        };

        const Self = @This();

        const State = union(enum) {
            start,
            op: Op,
            finished,
        };

        buf: Buf,
        fd: Fd,
        flags: i32,

        state: State,

        pub fn init(fd: Fd, buf: Buf, flags: i32) Self {
            return .{
                .buf = buf,
                .fd = fd,
                .flags = flags,
                .state = .start,
            };
        }

        pub fn poll(self: *Self, ctx: *const Context) Poll(Result(void, linux.E)) {
            while (true) {
                switch (self.state) {
                    .start => {
                        self.state = .{
                            .op = Op.init(self.fd, self.buf, self.flags),
                        };
                    },
                    .op => |*s| {
                        switch (s.poll(ctx)) {
                            .ready => |res| {
                                switch (res) {
                                    .ok => |r| {
                                        self.buf = self.buf[r..];
                                        if (self.buf.len > 0) {
                                            self.state = .{
                                                .op = Op.init(self.fd, self.buf, self.flags),
                                            };
                                        } else {
                                            self.state = .finished;
                                            return .{ .ready = .{ .ok = {} } };
                                        }
                                    },
                                    .err => |e| {
                                        if (e == linux.E.INTR) {
                                            s.* = Op.init(self.fd, self.buf, self.flags);
                                        } else {
                                            self.state = .finished;
                                            return .{ .ready = .{ .err = e } };
                                        }
                                    },
                                }
                            },
                            .pending => return .pending,
                        }
                    },
                    .finished => unreachable,
                }
            }
        }
    };
}

pub const SendExact = Exact(Send);
pub const RecvExact = Exact(Recv);
