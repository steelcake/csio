const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const Allocator = std.mem.Allocator;
const Instant = std.time.Instant;

const csio = @import("csio");
const fs = csio.fs;

pub const log_level: std.log.Level = .debug;

pub fn main() !void {
    const mem = alloc_thp(1 << 30) orelse unreachable;
    defer posix.munmap(mem);

    var fb_alloc = std.heap.FixedBufferAllocator.init(mem);
    const alloc = fb_alloc.allocator();

    var exec = try csio.Executor.init(.{
        .wq_fd = null,
        .alloc = alloc,
    });
    defer exec.deinit(alloc);

    var main_task = MainTask.init("testfile", 1 << 32);
    exec.run(main_task.task());
}

const MainTask = struct {
    const Self = @This();

    const State = union(enum) {
        start,
        setup_file: SetupFile,
        write: Write,
        read: Read,
        delete: fs.RemoveFile,
        finished,
    };

    path: [:0]const u8,
    file_size: u64,
    state: State,

    fn init(path: [:0]const u8, file_size: u64) Self {
        return .{
            .state = .start,
            .path = path,
            .file_size = file_size,
        };
    }

    fn poll(self: *Self, ctx: *const csio.Context) csio.Poll(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    self.state = .{
                        .setup_file = SetupFile.init(self.path, self.file_size),
                    };
                },
                .setup_file => |*sf| {
                    switch (sf.poll(ctx)) {
                        .ready => {
                            self.state = .{
                                .write = Write.init(self.path, self.file_size),
                            };
                        },
                        .pending => return .pending,
                    }
                },
                .write => |*w| {
                    switch (w.poll(ctx)) {
                        .ready => {
                            self.state = .{ .read = Read.init(self.path, self.file_size) };
                        },
                        .pending => return .pending,
                    }
                },
                .read => |*r| {
                    switch (r.poll(ctx)) {
                        .ready => {
                            self.state = .{ .delete = fs.RemoveFile.init(self.path) };
                        },
                        .pending => return .pending,
                    }
                },
                .delete => |*df| {
                    switch (df.poll(ctx)) {
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

const Read = struct {
    const Self = @This();

    const CONCURRENCY = 8;
    const IO_SIZE = 1 << 19;

    const State = union(enum) {
        start,
        open: struct { start_t: Instant, io: fs.Open },
        read: struct {
            fd_idx: u32,
            buffers: [CONCURRENCY][]u8,
            io: [CONCURRENCY]?fs.DirectRead,
            io_offset: [CONCURRENCY]u64,
            offset: u64,
            start_t: Instant,
        },
        close: struct { start_t: Instant, io: fs.Close },
        finished,
    };

    path: [:0]const u8,
    file_size: u64,
    state: State,

    fn init(path: [:0]const u8, file_size: u64) Self {
        return .{
            .path = path,
            .file_size = file_size,
            .state = .start,
        };
    }

    fn poll(self: *Self, ctx: *const csio.Context) csio.Poll(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    self.state = .{
                        .open = .{
                            .io = fs.Open.init(
                                self.path,
                                linux.O{
                                    .ACCMODE = .RDONLY,
                                    .DIRECT = true,
                                },
                                0,
                            ),
                            .start_t = Instant.now() catch unreachable,
                        },
                    };
                },
                .open => |*o| {
                    switch (o.io.poll(ctx)) {
                        .ready => |res| {
                            const fd = switch (res) {
                                .ok => |fd| fd,
                                .err => unreachable,
                            };

                            const now = Instant.now() catch unreachable;

                            std.log.info("It took {}us to open file for reading.", .{now.since(o.start_t) / 1000});

                            const fd_idx = ctx.register_fd(fd);

                            var buffers: [CONCURRENCY][]u8 = undefined;
                            for (0..CONCURRENCY) |idx| {
                                buffers[idx] = ctx.alloc_io_buf(IO_SIZE);
                            }

                            var io: [CONCURRENCY]?fs.DirectRead = undefined;
                            for (0..CONCURRENCY) |idx| {
                                io[idx] = null;
                            }

                            self.state = .{
                                .read = .{
                                    .fd_idx = fd_idx,
                                    .buffers = buffers,
                                    .io = io,
                                    .io_offset = undefined,
                                    .offset = 0,
                                    .start_t = now,
                                },
                            };
                        },
                        .pending => return .pending,
                    }
                },
                .read => |*r| {
                    var pending: u32 = 0;
                    io: for (0..CONCURRENCY) |idx| {
                        if (ctx.yield_if_needed()) {
                            return .pending;
                        }
                        const io_ptr = &r.io[idx];
                        while (true) {
                            if (io_ptr.*) |*io| {
                                switch (io.poll(ctx)) {
                                    .ready => |res| {
                                        switch (res) {
                                            .ok => |n_read| {
                                                if (n_read != IO_SIZE) {
                                                    std.debug.panic("unexpected number of bytes read. Expected {}, Read{}", .{ IO_SIZE, n_read });
                                                }
                                            },
                                            .err => |e| std.debug.panic("failed read: {}", .{e}),
                                        }

                                        const buf_typed: []const u64 = @ptrCast(@alignCast(r.buffers[idx]));
                                        const io_offset = r.io_offset[idx];
                                        var mismatch: bool = false;
                                        for (buf_typed, 0..) |x, i| {
                                            mismatch |= x != io_offset +% i;
                                        }
                                        std.debug.assert(!mismatch);

                                        io_ptr.* = null;
                                    },
                                    .pending => {
                                        pending += 1;
                                        continue :io;
                                    },
                                }
                            } else if (self.file_size == r.offset) {
                                continue :io;
                            } else {
                                io_ptr.* = fs.DirectRead.init(.{ .fixed = r.fd_idx }, r.buffers[idx], r.offset);
                                r.io_offset[idx] = r.offset;
                                r.offset += IO_SIZE;
                            }
                        }
                    }

                    if (r.offset == self.file_size and pending == 0) {
                        const now = Instant.now() catch unreachable;
                        const elapsed = @as(f64, @floatFromInt(now.since(r.start_t) / 1000)) / 1000000.0;
                        std.log.info("Finished reading in {d:.3}secs", .{elapsed});
                        const file_size_gb: f64 = @floatFromInt(self.file_size / (1 << 30));
                        std.log.info("Bandwidth: {d:.3}GB/s", .{file_size_gb / elapsed});
                        const num_io: f64 = @floatFromInt(self.file_size / IO_SIZE);
                        std.log.info("IOPS: {d:.3}", .{num_io / elapsed});

                        for (r.buffers) |b| {
                            ctx.free_io_buf(b);
                        }

                        const fd = ctx.unregister_fd(r.fd_idx);
                        self.state = .{ .close = .{ .io = fs.Close.init(fd), .start_t = now } };
                    } else {
                        return .pending;
                    }
                },
                .close => |*c| {
                    switch (c.io.poll(ctx)) {
                        .ready => {
                            const now = Instant.now() catch unreachable;
                            std.log.info("Finished closing file in {}us", .{now.since(c.start_t) / 1000});
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
};

const Write = struct {
    const Self = @This();

    const CONCURRENCY = 8;
    const IO_SIZE = 1 << 19;

    const State = union(enum) {
        start,
        open: struct { io: fs.Open, start_t: Instant },
        write: struct {
            fd_idx: u32,
            buffers: [CONCURRENCY][]u8,
            io: [CONCURRENCY]?fs.DirectWrite,
            offset: u64,
            start_t: Instant,
        },
        close: struct { io: fs.Close, start_t: Instant },
        finished,
    };

    path: [:0]const u8,
    file_size: u64,
    state: State,

    fn init(path: [:0]const u8, file_size: u64) Write {
        return .{
            .path = path,
            .file_size = file_size,
            .state = .start,
        };
    }

    fn poll(self: *Self, ctx: *const csio.Context) csio.Poll(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    const now = Instant.now() catch unreachable;
                    self.state = .{
                        .open = .{
                            .io = fs.Open.init(
                                self.path,
                                linux.O{
                                    .ACCMODE = .RDWR,
                                    .DSYNC = true,
                                    .DIRECT = true,
                                },
                                0,
                            ),
                            .start_t = now,
                        },
                    };
                },
                .open => |*o| {
                    switch (o.io.poll(ctx)) {
                        .ready => |res| {
                            const fd = switch (res) {
                                .ok => |fd| fd,
                                .err => unreachable,
                            };

                            const now = Instant.now() catch unreachable;

                            std.log.info("It took {}us to open file for writing", .{now.since(o.start_t) / 1000});

                            const fd_idx = ctx.register_fd(fd);

                            var buffers: [CONCURRENCY][]u8 = undefined;
                            for (0..CONCURRENCY) |idx| {
                                buffers[idx] = ctx.alloc_io_buf(IO_SIZE);
                            }

                            var io: [CONCURRENCY]?fs.DirectWrite = undefined;
                            for (0..CONCURRENCY) |idx| {
                                io[idx] = null;
                            }

                            self.state = .{
                                .write = .{
                                    .fd_idx = fd_idx,
                                    .buffers = buffers,
                                    .io = io,
                                    .offset = 0,
                                    .start_t = now,
                                },
                            };
                        },
                        .pending => return .pending,
                    }
                },
                .write => |*s| {
                    var pending: u32 = 0;
                    io: for (0..CONCURRENCY) |idx| {
                        if (ctx.yield_if_needed()) {
                            return .pending;
                        }
                        const io_ptr = &s.io[idx];
                        while (true) {
                            if (io_ptr.*) |*io| {
                                switch (io.poll(ctx)) {
                                    .ready => |res| {
                                        switch (res) {
                                            .ok => |n_wrote| {
                                                if (n_wrote != IO_SIZE) {
                                                    std.debug.panic("unexpected number of bytes written. Expected {}, Wrote {}", .{ IO_SIZE, n_wrote });
                                                }
                                            },
                                            .err => |e| std.debug.panic("failed write: {}", .{e}),
                                        }

                                        io_ptr.* = null;
                                    },
                                    .pending => {
                                        pending += 1;
                                        continue :io;
                                    },
                                }
                            } else if (self.file_size == s.offset) {
                                continue :io;
                            } else {
                                const buf = s.buffers[idx];
                                std.debug.assert(buf.len == IO_SIZE);

                                const buf_data: []u64 = @ptrCast(@alignCast(buf));
                                for (0..buf_data.len) |i| {
                                    buf_data.ptr[i] = i +% s.offset;
                                }

                                io_ptr.* = fs.DirectWrite.init(.{ .fixed = s.fd_idx }, buf, s.offset);
                                s.offset += IO_SIZE;
                            }
                        }
                    }

                    if (s.offset == self.file_size and pending == 0) {
                        const now = Instant.now() catch unreachable;
                        const elapsed = @as(f64, @floatFromInt(now.since(s.start_t) / 1000)) / 1000000.0;
                        std.log.info("Finished writing in {d:.3}secs", .{elapsed});
                        const file_size_gb: f64 = @floatFromInt(self.file_size / (1 << 30));
                        std.log.info("Bandwidth: {d:.3}GB/s", .{file_size_gb / elapsed});
                        const num_io: f64 = @floatFromInt(self.file_size / IO_SIZE);
                        std.log.info("IOPS: {d:.3}", .{num_io / elapsed});

                        for (s.buffers) |b| {
                            ctx.free_io_buf(b);
                        }

                        const fd = ctx.unregister_fd(s.fd_idx);
                        self.state = .{ .close = .{ .start_t = now, .io = fs.Close.init(fd) } };
                        return .ready;
                    }

                    return .pending;
                },
                .close => |*c| {
                    switch (c.io.poll(ctx)) {
                        .ready => {
                            const now = Instant.now() catch unreachable;
                            std.log.info("Finished closing file in {}us", .{now.since(c.start_t) / 1000});
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
        fallocate: struct { start_t: Instant, fd_idx: u32, io: fs.FAllocate },
        // close the file
        close: struct { start_t: Instant, io: fs.Close },
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

    fn poll(self: *Self, ctx: *const csio.Context) csio.Poll(void) {
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
                            switch (res) {
                                .err => |e| if (e != linux.E.NOENT) std.debug.panic("failed to remove file: {}", .{e}),
                                .ok => {},
                            }

                            const now = Instant.now() catch unreachable;

                            std.log.info("It took {}us to remove old file", .{now.since(s.start_t) / 1000});

                            self.state = .{
                                .open = .{
                                    .io = fs.Open.init(
                                        self.path,
                                        linux.O{
                                            .ACCMODE = .RDWR,
                                            .CREAT = true,
                                            .EXCL = true,
                                        },
                                        0o666,
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
                            const fd = switch (res) {
                                .ok => |fd| fd,
                                .err => unreachable,
                            };

                            const now = Instant.now() catch unreachable;

                            std.log.info("It took {}us to create new file", .{now.since(s.start_t) / 1000});

                            const fd_idx = ctx.register_fd(fd);

                            self.state = .{
                                .fallocate = .{
                                    .fd_idx = fd_idx,
                                    .io = fs.FAllocate.init(.{ .fixed = fd_idx }, 0, 0, self.size),
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
                            switch (res) {
                                .err => |e| std.debug.panic("failed to fallocate: {}", .{e}),
                                .ok => {},
                            }

                            const now = Instant.now() catch unreachable;
                            std.log.info("It took {}us to fallocate", .{now.since(s.start_t) / 1000});

                            const fd = ctx.unregister_fd(s.fd_idx);
                            self.state = .{ .close = .{ .start_t = now, .io = fs.Close.init(fd) } };
                        },
                        .pending => return .pending,
                    }
                },
                .close => |*c| {
                    switch (c.io.poll(ctx)) {
                        .ready => {
                            const now = Instant.now() catch unreachable;
                            std.log.info("Finished closing file in {}us", .{now.since(c.start_t) / 1000});
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
};

fn alloc_thp(size: usize) ?[]align(1 << 12) u8 {
    if (size == 0) {
        return null;
    }
    const alloc_size = fs.align_forward(size, 1 << 21);
    const page = mmap_wrapper(alloc_size, 0) orelse return null;
    posix.madvise(page.ptr, page.len, posix.MADV.HUGEPAGE) catch {
        posix.munmap(page);
        return null;
    };
    return page;
}

fn mmap_wrapper(size: usize, huge_page_flag: u32) ?[]align(1 << 12) u8 {
    if (size == 0) {
        return null;
    }
    const flags = linux.MAP{ .TYPE = .PRIVATE, .ANONYMOUS = true, .HUGETLB = huge_page_flag != 0, .POPULATE = true, .LOCKED = true };
    const flags_int: u32 = @bitCast(flags);
    const flags_f: linux.MAP = @bitCast(flags_int | huge_page_flag);
    const page = posix.mmap(null, size, posix.PROT.READ | posix.PROT.WRITE, flags_f, -1, 0) catch return null;
    return page;
}
