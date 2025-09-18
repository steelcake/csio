const std = @import("std");
const linux = std.os.linux;

const task_mod = @import("./task.zig");
const Context = task_mod.Context;
const Poll = task_mod.Poll;
const Result = task_mod.Result;
const Fd = task_mod.Fd;

pub const Socket = struct {
    domain: i32,
    socket_type: i32,
    protocol: i32,

    io_id: ?u64,
    finished: bool,

    pub fn init(domain: i32, socket_type: i32, protocol: i32) Socket {
        return .{
            .domain = domain,
            .socket_type = socket_type,
            .protocol = protocol,
            .io_id = null,
            .finished = false,
        };
    }

    pub fn poll(self: *Socket, ctx: *const Context) Poll(Result(linux.fd_t, linux.E)) {
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
            sqe.prep_socket(self.domain, self.socket_type, self.protocol);
            self.io_id = ctx.queue_io(sqe);
            return .pending;
        }
    }
};

pub const Bind = struct {
    fd: Fd,
    addr: *const linux.sockaddr,
    addr_len: u32,
    flags: u32,

    io_id: ?u64,
    finished: bool,

    pub fn init(fd: Fd, addr: *const linux.sockaddr, addr_len: u32, flags: u32) Bind {
        return .{
            .fd = fd,
            .addr = addr,
            .addr_len = addr_len,
            .flags = flags,
            .io_id = null,
            .finished = false,
        };
    }

    pub fn poll(self: *Bind, ctx: *const Context) Poll(Result(void, linux.E)) {
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
                    sqe.prep_bind(fd, self.addr, self.addr_len, self.flags);
                },
                .fixed => |idx| {
                    sqe.prep_bind(@intCast(idx), self.addr, self.addr_len, self.flags);
                    sqe.flags |= linux.IOSQE_FIXED_FILE;
                },
            }
            self.io_id = ctx.queue_io(sqe);
            return .pending;
        }
    }
};

pub const Listen = struct {};

pub const Connect = struct {};
