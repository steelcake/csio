const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const Allocator = std.mem.Allocator;
const Instant = std.time.Instant;
const Prng = std.Random.SplitMix64;

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
        write: struct { io: Write, fd: linux.fd_t },
        close_file: fs.Close,
        read: Read,
        delete_file: fs.RemoveFile,
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
                        .ready => |fd| {
                            self.state = .{
                                .write = .{ .io = Write.init(fd, self.file_size), .fd = fd },
                            };
                        },
                        .pending => return .pending,
                    }
                },
                .write => |*w| {
                    switch (w.io.poll(ctx)) {
                        .ready => {
                            self.state = .{
                                .close_file = fs.Close.init(w.fd),
                            };
                        },
                        .pending => return .pending,
                    }
                },
                .close_file => |*cf| {
                    switch (cf.poll(ctx)) {
                        .ready => {
                            self.state = .{ .read = Read.init(self.path, self.file_size) };
                        },
                        .pending => return .pending,
                    }
                },
                .read => |*r| {
                    switch (r.poll(ctx)) {
                        .ready => {
                            self.state = .{ .delete_file = fs.RemoveFile.init(self.path) };
                        },
                        .pending => return .pending,
                    }
                },
                .delete_file => |*df| {
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

const NUM_IO = 10;
const IO_SIZE = 1 << 20;

const Read = struct {
    const Self = @This();

    const State = union(enum) {
        start,
        open: struct { start_t: Instant, io: fs.Open },
        read: struct {
            fd: linux.fd_t,
            io: [NUM_IO]?fs.DioRead,
            io_offset: [NUM_IO]u64,
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
                            switch (res) {
                                .ok => |fd| {
                                    const now = Instant.now() catch unreachable;

                                    std.log.info("It took {}us to open file for reading.", .{now.since(o.start_t) / 1000});

                                    var io: [NUM_IO]?fs.DioRead = undefined;
                                    for (0..NUM_IO) |idx| {
                                        io[idx] = null;
                                    }

                                    self.state = .{
                                        .read = .{
                                            .fd = fd,
                                            .io = io,
                                            .io_offset = undefined,
                                            .offset = 0,
                                            .start_t = now,
                                        },
                                    };
                                },
                                .err => |e| std.debug.panic("failed to open file for reading: {}", .{e}),
                            }
                        },
                        .pending => return .pending,
                    }
                },
                .read => |*r| {
                    var pending: u32 = 0;
                    io: for (0..NUM_IO) |idx| {
                        if (ctx.yield_if_needed()) {
                            return .pending;
                        }
                        const io_ptr = &r.io[idx];
                        while (true) {
                            if (io_ptr.*) |*io| {
                                switch (io.poll(ctx)) {
                                    .ready => |res| {
                                        switch (res) {
                                            .ok => |buf| {
                                                defer fs.free_dio_buffer(ctx, buf);

                                                std.debug.assert(buf.data().len == IO_SIZE);

                                                const buf_typed: []const u64 = @ptrCast(@alignCast(buf.data()));

                                                var prng = Prng.init(r.io_offset[idx]);
                                                for (buf_typed) |x| {
                                                    std.debug.assert(x == prng.next());
                                                }

                                                io_ptr.* = null;
                                            },
                                            .err => |e| switch (e) {
                                                .short_read => unreachable,
                                                .os => |os_e| std.debug.panic("failed read: {}", .{os_e}),
                                            },
                                        }
                                    },
                                    .pending => {
                                        pending += 1;
                                        continue :io;
                                    },
                                }
                            } else if (self.file_size == r.offset) {
                                continue :io;
                            } else {
                                io_ptr.* = fs.DioRead.init(r.fd, r.offset, IO_SIZE);
                                r.io_offset[idx] = r.offset;
                                r.offset += IO_SIZE;
                            }
                        }
                    }

                    if (r.offset == self.file_size and pending == 0) {
                        const now = Instant.now() catch unreachable;
                        std.log.info("Finished reading in {}us", .{now.since(r.start_t) / 1000});
                        self.state = .{ .close = .{ .io = fs.Close.init(r.fd), .start_t = now } };
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

    const State = union(enum) {
        start,
        running: struct {
            buffers: [NUM_IO]fs.DioBuf,
            io: [NUM_IO]?fs.DioWrite,
            offset: u64,
            start_t: Instant,
        },
        finished,
    };

    fd: linux.fd_t,
    size: u64,
    state: State,

    fn init(fd: linux.fd_t, size: u64) Write {
        return .{
            .fd = fd,
            .size = size,
            .state = .start,
        };
    }

    fn poll(self: *Self, ctx: *const csio.Context) csio.Poll(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    var buffers: [NUM_IO]fs.DioBuf = undefined;
                    for (0..NUM_IO) |idx| {
                        const buf = fs.alloc_dio_buffer(ctx, IO_SIZE);
                        buffers[idx] = buf;
                    }

                    var io: [NUM_IO]?fs.DioWrite = undefined;
                    for (0..NUM_IO) |idx| {
                        io[idx] = null;
                    }

                    self.state = .{
                        .running = .{
                            .buffers = buffers,
                            .io = io,
                            .offset = 0,
                            .start_t = Instant.now() catch unreachable,
                        },
                    };
                },
                .running => |*s| {
                    var pending: u32 = 0;
                    io: for (0..NUM_IO) |idx| {
                        if (ctx.yield_if_needed()) {
                            return .pending;
                        }
                        const io_ptr = &s.io[idx];
                        while (true) {
                            if (io_ptr.*) |*io| {
                                switch (io.poll(ctx)) {
                                    .ready => |res| {
                                        switch (res) {
                                            .ok => {},
                                            .err => |e| switch (e) {
                                                .short_write => unreachable,
                                                .os => |os_e| std.debug.panic("failed write: {}", .{os_e}),
                                            },
                                        }

                                        io_ptr.* = null;
                                    },
                                    .pending => {
                                        pending += 1;
                                        continue :io;
                                    },
                                }
                            } else if (self.size == s.offset) {
                                continue :io;
                            } else {
                                const buf = s.buffers[idx];
                                const buf_data = buf.data();
                                std.debug.assert(buf_data.len == IO_SIZE);

                                var prng = Prng.init(s.offset);
                                const buf_out: []u64 = @ptrCast(@alignCast(buf_data));
                                for (buf_out) |*x| {
                                    x.* = prng.next();
                                }

                                io_ptr.* = fs.DioWrite.init(self.fd, buf, s.offset);
                                s.offset += IO_SIZE;
                            }
                        }
                    }

                    if (s.offset == self.size and pending == 0) {
                        const now = Instant.now() catch unreachable;
                        std.log.info("Finished writing in {}us", .{now.since(s.start_t) / 1000});
                        self.state = .finished;
                        return .ready;
                    }

                    return .pending;
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

    fn poll(self: *Self, ctx: *const csio.Context) csio.Poll(linux.fd_t) {
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
                                            .DSYNC = true,
                                            .DIRECT = true,
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
                            switch (res) {
                                .err => |e| std.debug.panic("failed to fallocate: {}", .{e}),
                                .ok => {},
                            }

                            const now = Instant.now() catch unreachable;
                            std.log.info("It took {}us to fallocate", .{now.since(s.start_t) / 1000});

                            const fd = s.fd;
                            self.state = .finished;
                            return .{ .ready = fd };
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
