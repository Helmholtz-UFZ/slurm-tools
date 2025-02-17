const std = @import("std");
const Compile = std.Build.Step.Compile;
const ResolvedTarget = std.Build.ResolvedTarget;
const OptimizedMode = std.builtin.OptimizeMode;
const Dependency = std.Build.Dependency;
const Module = std.Build.Module;
const slurm = @import("slurm");
const DependencyList = std.ArrayList(DependencyNamed);
const Allocator = std.mem.Allocator;

pub const DependencyNamed = struct {
    name: []const u8,
    data: *Dependency,
};

pub const Exe = struct {
    name: []const u8,
    target: ResolvedTarget,
    optimize: OptimizedMode,
    build: *std.Build,
    step: *Compile = undefined,
    deps: DependencyList,
    slurm_dep: *Dependency = undefined,
    backing_allocator: Allocator,
    path: []const u8 = undefined,

    pub fn default(b: *std.Build, name: []const u8, allocator: Allocator, target: ResolvedTarget, optimize: OptimizedMode) !Exe {
        var exe = Exe{
            .name = name,
            .target = target,
            .optimize = optimize,
            .build = b,
            .deps = DependencyList.init(allocator),
            .backing_allocator = allocator,
        };
        exe.slurm_dep = exe.stdDep("slurm");
        exe.path = try std.fmt.allocPrint(
            exe.backing_allocator,
            "src/{s}/main.zig",
            .{exe.name},
        );
        return exe;
    }

    pub fn install(self: *Exe) !void {
        self.step = self.build.addExecutable(.{
            .name = self.name,
            .root_source_file = self.build.path(self.path),
            .target = self.target,
            .optimize = self.optimize,
        });

        try slurm.setupSlurmPath(self.build, self.step, null);
        try self.addDep("slurm");
        self.linkSlurm();
        self.addImports();
        try self.addTests();
        self.build.installArtifact(self.step);
    }

    fn addTests(self: Exe) !void {
        const exe_unit_tests = self.build.addTest(.{
            .root_source_file = self.build.path(self.path),
            .target = self.target,
            .optimize = self.optimize,
        });
        const run_exe_unit_tests = self.build.addRunArtifact(exe_unit_tests);
        const test_name = try std.fmt.allocPrint(
            self.backing_allocator,
            "test-{s}",
            .{self.name},
        );
        const test_step = self.build.step(test_name, "Run unit tests");
        test_step.dependOn(&run_exe_unit_tests.step);
    }

    pub fn addImports(self: Exe) void {
        for (self.deps.items) |dep| {
            const mod: *Module = dep.data.module(dep.name);
            self.step.root_module.addImport(dep.name, mod);
        }
    }

    pub fn stdDep(self: Exe, name: []const u8) *Dependency {
        return self.build.dependency(name, .{
            .target = self.target,
            .optimize = self.optimize,
        });
    }

    pub fn namedDep(self: Exe, name: []const u8) DependencyNamed {
        return .{
            .name = name,
            .data = self.stdDep(name),
        };
    }

    pub fn addDep(self: *Exe, name: []const u8) !void {
        try self.deps.append(
            .{ .name = name, .data = self.stdDep(name) },
        );
    }

    pub fn linkSlurm(self: Exe) void {
        self.step.linkLibrary(self.slurm_dep.artifact("slurm"));
    }
};

fn snodestats(b: *std.Build, allocator: Allocator, target: ResolvedTarget, optimize: OptimizedMode) !void {
    var exe = try Exe.default(b, "snodestats", allocator, target, optimize);
    try exe.deps.appendSlice(&[_]DependencyNamed{
        exe.namedDep("prettytable"),
        exe.namedDep("clap"),
    });
    try exe.install();
}

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try snodestats(b, allocator, target, optimize);
}
