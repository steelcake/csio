const std = @import("std");
const linux = std.os.linux;
const Allocator = std.mem.Allocator;
const Instant = std.time.Instant;

const csio = @import("csio");
const fs = csio.fs;
const task = csio.task;

pub fn main() !void {
    const mem = std.heap.page_allocator.alloc(1 << 30);
    defer std.heap.page_allocator.free(mem);

    var fb_alloc = std.heap.FixedBufferAllocator.init(mem);
    const alloc = fb_alloc.allocator();

    var exec = try csio.executor.Executor.init(.{
        .wq_fd = null,
        .alloc = alloc,
    });

    const main_task = MainTask.init();
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

    alloc: Allocator,
    path: [:0]const u8,
    size: u64,
    state: State,

    fn init(path: [:0]const u8, size: u64, alloc: Allocator) Self {
        return .{
            .state = .start,
            .alloc = alloc,
            .path = path,
            .size = size,
        };
    }

    fn poll(self: *Self, ctx: csio.task.Context) csio.task.PollResult(void) {
        while (true) {
            switch (self.*) {
                .init => {
                    self.* = .{ .delete = .{
                        .start_t = Instant.now(),
                        .io = csio.file.unlink(FILE_PATH),
                    } };
                },
                .delete => |*s| {
                    switch (s.io.poll(ctx)) {
                        .ready => |res| {
                            std.log.info("it took {}us to run delete", .{Instant.now().since(s.start_t) / 1000});
                            res catch |e| {
                                std.log.err("failed to delete file: {any}", e);
                            };
                            self.* = .{
                                .create = .{
                                    .io = csio.file.DioFile.open(),
                                },
                            };
                        },
                        .pending => return .pending,
                    }
                },
            }
        }
    }

    fn poll_fn(ptr: *anyopaque, ctx: csio.task.Context) csio.task.PollResult(void) {
        const self: *Self = @ptrCast(ptr);
        return self.poll(ctx);
    }

    fn task(self: *Self) csio.task.Task {
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

    fn poll(self: *Self, ctx: task.Context) task.PollResult(linux.fd_t) {
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
                            
                            std.log.info("It took {}us to remove old file", .{ now.since(s.start_t) / 1000 });

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

                            std.log.info("It took {}us to create new file", .{ now.since(s.start_t) / 1000 });

                            self.state = .{
                                .fallocate = .{ .fd = fd, .io = fs.FAllocate.init(fd, 0, 0, self.size), .start_t = now, },
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

