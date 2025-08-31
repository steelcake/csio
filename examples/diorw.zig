const std = @import("std");
const linux = std.os.linux;
const Allocator = std.mem.Allocator;
const Instant = std.time.Instant;

const csio = @import("csio");
const fs = csio.fs;

pub fn main() !void {
    const mem = try std.heap.page_allocator.alloc(u8, 1 << 30);
    defer std.heap.page_allocator.free(mem);

    var fb_alloc = std.heap.FixedBufferAllocator.init(mem);
    const alloc = fb_alloc.allocator();

    var exec = try csio.Executor.init(.{
        .wq_fd = null,
        .alloc = alloc,
    });
    defer exec.deinit(alloc);

    var main_task = MainTask.init("testfile", 1 << 34);
    exec.run(main_task.task());
}

const MainTask = struct {
    const Self = @This();

    const State = union(enum) {
        start,
        setup_file: SetupFile,
        close_file: fs.Close,
        finished,
    };

    path: [:0]const u8,
    size: u64,
    state: State,

    fn init(path: [:0]const u8, size: u64) Self {
        return .{
            .state = .start,
            .path = path,
            .size = size,
        };
    }

    fn poll(self: *Self, ctx: *const csio.Context) csio.PollResult(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    self.state = .{
                        .setup_file = SetupFile.init(self.path, self.size),
                    };
                },
                .setup_file => |*sf| {
                    switch (sf.poll(ctx)) {
                        .ready => |fd| {
                            self.state = .{
                                .close_file = fs.Close.init(fd),
                            };
                        },
                        .pending => return .pending,
                    }
                },
                .close_file => |*cf| {
                    switch (cf.poll(ctx)) {
                        .ready => {
                            self.state = .{ .finished = {} };
                            return .ready;
                        },
                        .pending => return .pending,
                    }
                },
                .finished => unreachable,
            }
        }
    }

    fn poll_fn(ptr: *anyopaque, ctx: *const csio.Context) csio.PollResult(void) {
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

const SetupFile = struct {
    const Self = @This();

    const State = union(enum) {
        start,
        // delete old file if it exists
        remove: struct { start_t: Instant, io: fs.RemoveFile },
        // create and open file
        open: struct { start_t: Instant, io: fs.Open },
        // reserve space for file
        fallocate: struct { start_t: Instant, fd: linux.fd_t, io: fs.FAllocate },
        finished,
    };

    path: [:0]const u8,
    size: u64,
    state: State,

    fn init(path: [:0]const u8, size: u64) Self {
        return .{
            .path = path,
            .size = size,
            .state = .start,
        };
    }

    fn poll(self: *Self, ctx: *const csio.Context) csio.PollResult(linux.fd_t) {
        while (true) {
            switch (self.state) {
                .start => {
                    self.state = .{
                        .remove = .{
                            .io = fs.RemoveFile.init(self.path),
                            .start_t = Instant.now() catch unreachable,
                        },
                    };
                },
                .remove => |*s| {
                    switch (s.io.poll(ctx)) {
                        .ready => |res| {
                            res catch unreachable;

                            const now = Instant.now() catch unreachable;

                            std.log.info("It took {}us to remove old file", .{now.since(s.start_t) / 1000});

                            self.state = .{
                                .open = .{
                                    .io = fs.Open.init(
                                        self.path,
                                        linux.O{
                                            .CREAT = true,
                                            .EXCL = true,
                                            .DSYNC = true,
                                        },
                                        600,
                                    ),
                                    .start_t = now,
                                },
                            };
                        },
                        .pending => return .pending,
                    }
                },
                .open => |*s| {
                    switch (s.io.poll(ctx)) {
                        .ready => |res| {
                            const fd = res catch unreachable;

                            const now = Instant.now() catch unreachable;

                            std.log.info("It took {}us to create new file", .{now.since(s.start_t) / 1000});

                            self.state = .{
                                .fallocate = .{
                                    .fd = fd,
                                    .io = fs.FAllocate.init(fd, 0, 0, self.size),
                                    .start_t = now,
                                },
                            };
                        },
                        .pending => return .pending,
                    }
                },
                .fallocate => |*s| {
                    switch (s.io.poll(ctx)) {
                        .ready => |res| {
                            res catch unreachable;

                            const now = Instant.now() catch unreachable;
                            std.log.info("It took {}us to fallocate", .{now.since(s.start_t) / 1000});

                            self.state = .finished;
                            return .{ .ready = s.fd };
                        },
                        .pending => return .pending,
                    }
                },
                .finished => unreachable,
            }
        }
    }
};
