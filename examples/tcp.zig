const std = @import("std");
const linux = std.os.linux;
const Allocator = std.mem.Allocator;
const Instant = std.time.Instant;

const csio = @import("csio");
const net = csio.net;

pub const log_level: std.log.Level = .debug;

const PORT = 1131;
const NUM_CLIENTS = 64;
const NUM_PINGPONGS = 1 << 20;
const MESSAGE_SIZE = 1024;

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
        run_server: struct { inner: RunServer, server_fd_idx: u32, clients: []Client },
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

                    self.state = .run_server;
                },
                .run_server => |*s| {
                    const fd = ctx.unregister_fd(fd_idx);
                    self.state = .{
                        .close_server = net.Close.init(fd),
                    };
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

const ClientPingPong = union(enum) {
    start,
    send: net.Send,
    recv: net.Recv,
    finished,

    // fn init(fd_idx: u32, data: []u8)
};

const Client = struct {
    const Self = @This();

    const State = union(enum) {
        start,
        socket: struct { io: net.Socket, start_t: Instant },
        connect: struct { fd_idx: u32, io: net.Connect, start_t: Instant },
        pingpong: struct {
            inner: PingPong,
            counter: u8,
        },
        close: net.Close,
    };

    state: State,
    data: []u64,

    fn init(alloc: Allocator) Self {}

    fn poll(self: *Self, ctx: *const csio.Context) csio.Poll(void) {}

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

const RunServer = struct {};

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
