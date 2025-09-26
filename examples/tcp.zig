const std = @import("std");
const linux = std.os.linux;
const Allocator = std.mem.Allocator;
const Instant = std.time.Instant;

const csio = @import("csio");
const net = csio.net;

pub const log_level: std.log.Level = .debug;

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

    var server = Server.init();
    exec.run(server.task());
}

const Server = struct {
    const Self = @This();

    const State = union(enum) {
        start,
        setup_socket: SetupSocket,
    };

    state: State,

    fn init() Self {
        return .{
            .state = .start,
        };
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

const SetupSocket = struct {
    const Self = @This();

    const State = union(enum) {
        start,
        socket: struct { io: net.Socket, start_t: Instant },
        bind: struct { io: net.Bind, fd_idx: u32, start_t: Instant, addr: *std.net.Address},
    };

    state: State,
    alloc: Allocator,

    fn poll(self: *Self, ctx: *const csio.Context) csio.Poll(u32) {
        while (true) {
            switch (self.state) {
                .start => {
                    const now = Instant.now() catch unreachable;
                    self.state = .{ .socket = .{
                        .io = net.Socket.init(linux.AF.INET, linux.SOCK.STREAM | linux.SOCK.CLOEXEC, 0),
                        .start_t = now,
                    } };
                },
                .socket => |s| {
                    switch (s.io.poll(ctx)) {
                        .ready => |res| {
                            const now = Instant.now() catch unreachable;
                            const fd_idx = switch (res) {
                                .ok => |fd| ctx.register_fd(fd),
                                .err => |e| std.debug.panic("failed to create socket: {}", .{e}),
                            };

                            std.log.info("created socket in {}us", .{ now.since(s.start_t) / 1000 });

                            const addr = self.alloc.create(std.net.Address) catch unreachable;


                            std.net.Address.

                            self.state = .{
                                .bind = .{
                                    .start_t = now,
                                    .io = net.Bind.init(.{ .fixed = fd_idx, }, @ptrCast(addr), ),
                                    .fd_idx = fd_idx,
                                },
                            };
                        },
                        .pending => return .pending,
                    }
                },
            }
        }
    }
};
