const std = @import("std");
const linux = std.os.linux;
const Allocator = std.mem.Allocator;
const Instant = std.time.Instant;

const csio = @import("csio");

pub fn main() !void {
    const mem = std.heap.page_allocator.alloc(1 << 30);
    defer std.heap.page_allocator.free(mem);

    var fb_alloc = std.heap.FixedBufferAllocator.init(mem);
    const alloc = fb_alloc.allocator();

    var exec = try csio.executor.Executor.init(.{
        .wq_fd = null,
        .alloc = alloc,
    });

    const main_task = MainTask{ .init = {} };
    exec.run(main_task.task());
}

const MainTask = union(enum) {
    const Self = @This();

    const CONCURRENCY = 4;
    const FILE_SIZE = 1 << 34;
    const FILE_PATH = "testfile";
    const IO_SIZE = 1 << 22;

    init: void,
    delete: struct { start_t: Instant, io: csio.file.UnlinkAt },
    create: struct { start_t: Instant, io: csio.file.Open },
    fallocate: struct {
        start_t: Instant,
        file: csio.file.DioFile,
        io: csio.file.FAllocate,
    },
    write: struct {
        start_t: Instant,
        file: csio.file.DioFile,
        io: [CONCURRENCY]csio.file.DioWrite,
        io_buf: [CONCURRENCY]csio.file.DioBuf,
        io_is_running: [CONCURRENCY]bool,
        current_offset: u64,
    },
    read: struct {
        start_t: Instant,
        file: csio.file.DioFile,
        io: [CONCURRENCY]csio.file.DioRead,
        io_buf: [CONCURRENCY]csio.file.DioBuf,
        io_offset: [CONCURRENCY]u64,
        io_size: [CONCURRENCY]u32,
        io_is_running: [CONCURRENCY]bool,
    },
    close: struct { start_t: Instant, io: csio.file.Close },

    fn poll(self: *Self, ctx: csio.task.Context) csio.task.PollResult(!void) {
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

        switch (self.poll(ctx)) {
            .ready => |res| {
                res catch unreachable;
                return .ready;
            },
            .pending => return .pending,
        }
    }

    fn task(self: *Self) csio.task.Task {
        return .{
            .ptr = self,
            .poll_fn = Self.poll_fn,
        };
    }
};
