const std = @import("std");
const linux = std.os.linux;
const Allocator = std.mem.Allocator;
const Instant = std.time.Instant;

const csio = @import("csio");
const net = csio.net;

pub const log_level: std.log.Level = .debug;

const PORT = 1131;
const NUM_CLIENTS = 64;
const MESSAGES_PER_CLIENT = 1 << 20;
const MESSAGE_SIZE = 1 << 15;

pub fn main() !void {
    const mem = try std.heap.page_allocator.alloc(u8, 1 << 30);
    defer std.heap.page_allocator.free(mem);

    var fb_alloc = std.heap.FixedBufferAllocator.init(mem);
    const alloc = fb_alloc.allocator();

    var exec = try csio.Executor.init(.{
        .direct_io_alloc_buf = &.{},
        .wq_fd = null,
        .alloc = alloc,
    });
    defer exec.deinit(alloc);

    var main_task = MainTask.init(alloc);
    exec.run(main_task.task());
}

const MainTask = struct {
    const Self = @This();

    const State = union(enum) {
        start,
        setup_server: SetupServer,
        start_clients: struct { server_fd_idx: u32 },
        run_server: struct {
            inner: RunServer,
            server_fd_idx: u32,
            clients: []Client,
        },
        close_server: net.Close,
        finished,
    };

    state: State,
    alloc: Allocator,

    fn init(alloc: Allocator) Self {
        return .{
            .state = .start,
            .alloc = alloc,
        };
    }

    fn poll(self: *Self, ctx: *const csio.Context) csio.Poll(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    self.state = .{
                        .setup_server = SetupServer.init(),
                    };
                },
                .setup_server => |*f| {
                    switch (f.poll(ctx, self.alloc)) {
                        .ready => |server_fd_idx| {
                            self.state = .{
                                .start_clients = .{ .server_fd_idx = server_fd_idx },
                            };
                        },
                        .pending => return .pending,
                    }
                },
                .start_clients => |*s| {
                    const clients = self.alloc.alloc(Client, NUM_CLIENTS) catch unreachable;
                    for (clients) |*c| {
                        c.* = Client.init();
                        ctx.spawn(c.task());
                    }

                    const server_fd_idx = s.server_fd_idx;

                    self.state = .{
                        .run_server = .{
                            .clients = clients,
                            .server_fd_idx = server_fd_idx,
                            .inner = RunServer.init(server_fd_idx),
                        },
                    };
                },
                .run_server => |*s| {
                    switch (s.inner.poll(ctx, self.alloc)) {
                        .ready => {
                            self.alloc.free(s.clients);
                            const fd = ctx.unregister_fd(s.server_fd_idx);
                            self.state = .{
                                .close_server = net.Close.init(fd),
                            };
                        },
                        .pending => return .pending,
                    }
                },
                .close_server => |*f| {
                    switch (f.poll(ctx)) {
                        .ready => {
                            self.state = .finished;
                            return .ready;
                        },
                        .pending => return .pending,
                    }
                },
                .finished => unreachable,
            }
        }
    }

    fn poll_fn(ptr: *anyopaque, ctx: *const csio.Context) csio.Poll(void) {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.poll(ctx);
    }

    fn task(self: *Self) csio.Task {
        return .{
            .ptr = self,
            .poll_fn = Self.poll_fn,
        };
    }
};

const ClientPingPong = struct {
    const Self = @This();

    const State = union(enum) {
        start,
        send: net.SendExact,
        recv: net.RecvExact,
        finished,
    };

    data: []u8,
    fd_idx: u32,
    state: State,

    fn init(fd_idx: u32, data: []u8) Self {
        return .{
            .data = data,
            .fd_idx = fd_idx,
            .state = .start,
        };
    }

    fn poll(self: *Self, ctx: *const csio.Context) csio.Poll(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    self.state = .{
                        .send = net.SendExact.init(.{ .fixed = self.fd_idx }, self.data, 0),
                    };
                },
                .send => |*s| {
                    switch (s.poll(ctx)) {
                        .ready => |res| {
                            switch (res) {
                                .ok => {},
                                .err => |e| std.debug.panic("failed to send: {}", .{e}),
                            }

                            self.state = .{
                                .recv = net.RecvExact.init(.{ .fixed = self.fd_idx }, self.data, 0),
                            };
                        },
                        .pending => return .pending,
                    }
                },
                .recv => |*s| {
                    switch (s.poll(ctx)) {
                        .ready => |res| {
                            switch (res) {
                                .ok => {},
                                .err => |e| std.debug.panic("failed to recv: {}", .{e}),
                            }

                            self.state = .finished;
                            return .{ .ready = {} };
                        },
                        .pending => return .pending,
                    }
                },
                .finished => unreachable,
            }
        }
    }
};

const Client = struct {
    const Self = @This();

    const State = union(enum) {
        start,
        socket: struct { io: net.Socket, start_t: Instant },
        connect: struct {
            fd_idx: u32,
            io: net.Connect,
            start_t: Instant,
            addr: *std.net.Address,
        },
        pingpong: struct {
            fd_idx: u32,
            inner: ClientPingPong,
            counter: u32,
            start_t: Instant,
        },
        close: net.Close,
        finished,
    };

    state: State,
    data: []u8,

    fn init(alloc: Allocator) Self {
        const data = alloc.alloc(u8, MESSAGE_SIZE) catch unreachable;
        fill_data(data, 0);

        return .{
            .state = .start,
            .data = data,
        };
    }

    fn poll(self: *Self, ctx: *const csio.Context, alloc: Allocator) csio.Poll(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    self.state = .{
                        .socket = .{
                            .io = net.Socket.init(linux.AF.INET, linux.SOCK.STREAM | linux.SOCK.CLOEXEC, 0),
                            .start_t = Instant.now() catch unreachable,
                        },
                    };
                },
                .socket => |*s| {
                    switch (s.io.poll(ctx)) {
                        .ready => |res| {
                            const now = Instant.now() catch unreachable;

                            const fd_idx = switch (res) {
                                .ok => |fd| ctx.register_fd(fd),
                                .err => |e| std.debug.panic("failed to create socket for client: {}", .{e}),
                            };

                            std.log.info("created socket for client in {}us", .{now.since(s.start_t) / 1000});

                            const addr = alloc.create(std.net.Address) catch unreachable;
                            addr.* = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, PORT);

                            self.state = .{
                                .connect = .{
                                    .fd_idx = fd_idx,
                                    .io = net.Connect.init(.{ .fixed = fd_idx }, &addr.any, addr.getOsSockLen()),
                                    .start_t = now,
                                    .addr = addr,
                                },
                            };
                        },
                        .pending => return .pending,
                    }
                },
                .connect => |*s| {
                    switch (s.io.poll(ctx)) {
                        .ready => |res| {
                            const now = Instant.now() catch unreachable;

                            alloc.destroy(s.addr);
                            switch (res) {
                                .ok => {},
                                .err => |e| std.debug.panic("failed to bind: {}", .{e}),
                            }

                            std.log.info("connected client in {}us", .{now.since(s.start_t) / 1000});

                            const fd_idx = s.fd_idx;

                            self.state = .{
                                .pingpong = .{
                                    .inner = ClientPingPong.init(fd_idx, self.data),
                                    .counter = 0,
                                    .start_t = now,
                                    .fd_idx = fd_idx,
                                },
                            };
                        },
                        .pending => return .pending,
                    }
                },
                .pingpong => |*s| {
                    switch (s.inner.poll(ctx)) {
                        .ready => {
                            check_data(self.data, s.counter);

                            if (s.counter < MESSAGES_PER_CLIENT) {
                                s.counter += 1;
                                fill_data(self.data, s.counter);
                                s.inner = ClientPingPong.init(s.fd_idx, self.data);
                            } else {
                                const now = Instant.now() catch unreachable;
                                std.log.info("finished client work in {}us", .{now.since(s.start_t) / 1000});
                                const fd = ctx.unregister_fd(s.fd_idx);
                                self.state = .{
                                    .close = net.Close.init(fd),
                                };
                            }
                        },
                        .pending => return .pending,
                    }
                },
                .close => |*s| {
                    switch (s.poll(ctx)) {
                        .ready => |res| {
                            switch (res) {
                                .ok => {},
                                .err => |e| std.debug.panic("failed to close client socket: {}", .{e}),
                            }

                            self.state = .finished;
                        },
                        .pending => return .pending,
                    }
                },
                .finished => unreachable,
            }
        }
    }

    fn poll_fn(ptr: *anyopaque, ctx: *const csio.Context) csio.Poll(void) {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.poll(ctx);
    }

    fn task(self: *Self) csio.Task {
        return .{
            .ptr = self,
            .poll_fn = Self.poll_fn,
        };
    }
};

const ServerPingPong = struct {
    const Self = @This();

    const State = union(enum) {
        start,
        recv: net.RecvExact,
        send: net.SendExact,
        close: net.Close,
        finished,
    };

    counter: u32,
    conn_fd_idx: u32,
    state: State,
    buf: []u8,

    fn init(conn_fd_idx: u32, alloc: Allocator) Self {
        const buf = alloc.alloc(u8, MESSAGE_SIZE) catch unreachable;
        return .{
            .state = .start,
            .counter = 0,
            .conn_fd_idx = conn_fd_idx,
            .buf = buf,
        };
    }

    fn poll(self: *Self, ctx: *const csio.Context) csio.Poll(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    self.state = .{
                        .recv = net.RecvExact.init(.{ .fixed = self.conn_fd_idx }, self.buf, 0),
                    };
                },
                .recv => |*f| {
                    switch (f.poll(ctx)) {
                        .ready => {
                            check_data(self.buf, self.counter);

                            self.state = .{
                                .recv = net.SendExact.init(.{ .fixed = self.conn_fd_idx }, self.buf, 0),
                            };
                        },
                        .pending => return .pending,
                    }
                },
                .send => |*f| {
                    switch (f.poll(ctx)) {
                        .ready => {
                            if (self.counter < MESSAGES_PER_CLIENT) {
                                self.counter += 1;

                                self.state = .{
                                    .recv = net.RecvExact.init(.{ .fixed = self.conn_fd_idx }, self.buf, 0),
                                };
                            } else {
                                const fd = ctx.unregister_fd(self.conn_fd_idx);
                                self.state = .{
                                    .close = net.Close.init(fd),
                                };
                            }
                        },
                        .pending => return .pending,
                    }
                },
                .close => |*f| {
                    switch (f.poll(ctx)) {
                        .ready => |res| {
                            switch (res) {
                                .ok => {},
                                .err => |e| std.debug.panic("failed to close conn socket: {}", .{e}),
                            }

                            self.state = .finished;
                            return .{ .ready = {} };
                        },
                        .pending => return .pending,
                    }
                },
                .finished => unreachable,
            }
        }
    }
};

const RunServer = struct {
    const Self = @This();

    const State = union(enum) {
        start,
        running: struct {
            pingpong: []ServerPingPong,
            num_active: u32,
            num_finished: u32,
        },
        finished,
    };

    state: State,
    server_fd_idx: u32,
    accept: net.Accept,

    fn init(server_fd_idx: u32) Self {
        return .{
            .server_fd_idx = server_fd_idx,
            .state = .start,
            .accept = net.Accept.init(.{ .fixed = server_fd_idx }, null, null, 0),
        };
    }

    fn poll(self: *Self, ctx: *const csio.Context, alloc: Allocator) csio.Poll(void) {
        switch (self.state) {
            .start => {
                const pp = alloc.alloc(ServerPingPong, NUM_CLIENTS) catch unreachable;
                self.state = .{
                    .running = .{
                        .pingpong = pp,
                        .num_active = 0,
                        .num_finished = 0,
                    },
                };
            },
            .running => |*s| {
                var idx: u32 = 0;
                while (idx < s.num_active) {
                    switch (s.pingpong[idx].poll(ctx)) {
                        .ready => {
                            s.num_active -= 1;
                            s.num_finished += 1;
                            s.pingpong[idx] = s.pingpong[s.num_active];
                        },
                        .pending => {
                            idx += 1;
                        },
                    }
                }

                switch (self.accept.poll(ctx)) {
                    .ready => |res| {},
                    .pending => {},
                }
            },
            .finished => unreachable,
        }
    }
};

const SetupServer = struct {
    const Self = @This();

    const State = union(enum) {
        start,
        socket: struct { io: net.Socket, start_t: Instant },
        bind: struct { io: net.Bind, fd_idx: u32, start_t: Instant, addr: *std.net.Address },
        listen: struct { io: net.Listen, fd_idx: u32, start_t: Instant },
        finished,
    };

    state: State,

    fn init() Self {
        return .{
            .state = .start,
        };
    }

    fn poll(self: *Self, ctx: *const csio.Context, alloc: Allocator) csio.Poll(u32) {
        while (true) {
            switch (self.state) {
                .start => {
                    const now = Instant.now() catch unreachable;
                    self.state = .{ .socket = .{
                        .io = net.Socket.init(linux.AF.INET, linux.SOCK.STREAM | linux.SOCK.CLOEXEC, 0),
                        .start_t = now,
                    } };
                },
                .socket => |*s| {
                    switch (s.io.poll(ctx)) {
                        .ready => |res| {
                            const now = Instant.now() catch unreachable;
                            const fd_idx = switch (res) {
                                .ok => |fd| ctx.register_fd(fd),
                                .err => |e| std.debug.panic("failed to create socket: {}", .{e}),
                            };

                            std.log.info("created socket in {}us", .{now.since(s.start_t) / 1000});

                            const addr = alloc.create(std.net.Address) catch unreachable;
                            addr.* = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, PORT);

                            self.state = .{
                                .bind = .{
                                    .start_t = now,
                                    .io = net.Bind.init(.{
                                        .fixed = fd_idx,
                                    }, &addr.any, addr.getOsSockLen(), 0),
                                    .fd_idx = fd_idx,
                                    .addr = addr,
                                },
                            };
                        },
                        .pending => return .pending,
                    }
                },
                .bind => |*s| {
                    switch (s.io.poll(ctx)) {
                        .ready => |res| {
                            alloc.destroy(s.addr);
                            switch (res) {
                                .ok => {},
                                .err => |e| std.debug.panic("failed to bind: {}", .{e}),
                            }

                            const now = Instant.now() catch unreachable;

                            std.log.info("bind socket in {}us", .{now.since(s.start_t) / 1000});

                            self.state = .{
                                .listen = .{
                                    .start_t = now,
                                    .io = net.Listen.init(.{ .fixed = s.fd_idx }, NUM_CLIENTS),
                                    .fd_idx = s.fd_idx,
                                },
                            };
                        },
                        .pending => return .pending,
                    }
                },
                .listen => |*s| {
                    switch (s.io.poll(ctx)) {
                        .ready => |res| {
                            switch (res) {
                                .ok => {},
                                .err => |e| std.debug.panic("failed to listen: {}", .{e}),
                            }

                            const fd_idx = s.fd_idx;
                            self.state = .finished;

                            return .{ .ready = fd_idx };
                        },
                        .pending => return .pending,
                    }
                },
                .finished => unreachable,
            }
        }
    }
};

fn fill_data(data: []u8, counter: u32) void {
    std.debug.assert(data.len == MESSAGE_SIZE);
    var v = counter *% MESSAGE_SIZE;
    for (0..MESSAGE_SIZE) |idx| {
        data.ptr[idx] = v;
        v +%= 1;
    }
}

fn check_data(data: []const u8, counter: u32) void {
    std.debug.assert(data.len == MESSAGE_SIZE);
    var v = counter *% MESSAGE_SIZE;
    for (0..MESSAGE_SIZE) |idx| {
        std.debug.assert(data.ptr[idx] == v);
        v +%= 1;
    }
}
