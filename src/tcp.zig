const std = @import("std");
const linux = std.os.linux;

const task_mod = @import("./task.zig");
const Context = task_mod.Context;
const Poll = task_mod.Poll;
const Result = task_mod.Result;

// pub const Listen = struct {
//     pub fn poll(self: *Listen, ctx: *const Context) Poll(Result(linux.fd_t, linux.E)) {
//         while (true) {
//             linux.io_uring_sqe.prep_listen()
//         }
//     }
// };
