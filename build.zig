const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("csio", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
    });

    const mod_tests = b.addTest(.{
        .root_module = mod,
    });

    const run_mod_tests = b.addRunArtifact(mod_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);

    const fuzz = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/fuzz.zig"),
            .target = target,
            .optimize = optimize,
        }),
        // Required for running fuzz tests
        // https://github.com/ziglang/zig/issues/23423
        .use_llvm = true,
    });

    const run_fuzz = b.addRunArtifact(fuzz);

    const fuzz_step = b.step("fuzz", "run fuzz tests");
    fuzz_step.dependOn(&run_fuzz.step);

    const Example = enum {
        direct_io,
        tcp,
    };
    const example_option = b.option(Example, "example", "Example to run, default is direct_io") orelse Example.direct_io;
    const example_step = b.step("example", "Run example");
    const example = b.addExecutable(.{
        .name = b.fmt("{s}_example", .{@tagName(example_option)}),
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                b.fmt("examples/{s}.zig", .{@tagName(example_option)}),
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "csio", .module = mod },
            },
        }),
    });

    const example_run = b.addRunArtifact(example);
    example_step.dependOn(&example_run.step);
}
