const std = @import("std");
const slurm = @import("slurm");
const pt = @import("prettytable");
const clap = @import("clap");
const allocPrint = std.fmt.allocPrint;
const Allocator = std.mem.Allocator;

fn humanize(allocator: Allocator, val: u64) ![]const u8 {
    const units = [_][]const u8{ "M", "G", "T", "P", "E", "Z" };
    var fl: f64 = @floatFromInt(val);

    for (units) |unit| {
        if (@abs(fl) < 1024.0) {
            return try allocPrint(
                allocator,
                "{d:.2}{s}",
                .{ fl, unit },
            );
        }
        fl /= 1024.0;
    }

    return try allocPrint(
        allocator,
        "{d:.2}{s}",
        .{ fl, "Y" },
    );
}

fn to_percent(comptime T: type, numerator: anytype, denominator: anytype) T {
    const n: T = @floatFromInt(numerator);
    const d: T = @floatFromInt(denominator);
    return (n / d) * 100.0;
}

pub fn show_parts() void {}

fn show_node_total_util(allocator: Allocator, util: slurm.Node.Utilization) !void {
    var tstats = pt.Table.init(allocator);
    defer tstats.deinit();

    tstats.setFormat(pt.FORMAT_NO_BORDER);
    try tstats.setTitle(&.{ "Resource", "Total", "Alloc", "Idle" });

    const alloc_cpus_percent = to_percent(f64, util.alloc_cpus, util.total_cpus);
    const idle_cpus_percent = to_percent(f64, util.idle_cpus, util.total_cpus);

    try tstats.addRow(&.{
        "CPU",
        try allocPrint(allocator, "{d}", .{util.total_cpus}),
        try allocPrint(allocator, "{d} ({d:.2}%)", .{ util.alloc_cpus, alloc_cpus_percent }),
        try allocPrint(allocator, "{d} ({d:.2}%)", .{ util.idle_cpus, idle_cpus_percent }),
    });

    const alloc_mem_percent = to_percent(f64, util.alloc_memory, util.real_memory);
    const idle_mem_percent = to_percent(f64, util.idle_memory, util.real_memory);
    const total_mem_hum = try humanize(allocator, util.real_memory);
    const alloc_mem_hum = try humanize(allocator, util.alloc_memory);
    const idle_mem_hum = try humanize(allocator, util.idle_memory);

    try tstats.addRow(&.{
        "Memory",
        total_mem_hum,
        try allocPrint(allocator, "{s} ({d:.2}%)", .{ alloc_mem_hum, alloc_mem_percent }),
        try allocPrint(allocator, "{s} ({d:.2}%)", .{ idle_mem_hum, idle_mem_percent }),
    });

    try tstats.printstd();
}

pub fn show_nodes(allocator: Allocator, res: anytype, stdout: anytype) !void {
    var table = pt.Table.init(allocator);
    defer table.deinit();
    const arg_free = res.args.free != 0;

    if (arg_free) {
        try table.setTitle(&.{ "Nodename", "IdleCPUs", "IdleMemory" });
    } else {
        try table.setTitle(&.{ "Nodename", "AllocCPUs", "IdleCPUs", "AllocMemory", "IdleMemory" });
    }

    var node_resp = try slurm.Node.load_all();
    defer node_resp.deinit();

    var total_util = slurm.Node.Utilization{};

    var node_iter = node_resp.iter();
    while (node_iter.next()) |node| {
        const pdata = node.parse_c_ptr();
        const idle_cpus = try allocPrint(allocator, "{d}", .{pdata.idle_cpus});
        const alloc_cpus = try allocPrint(allocator, "{d}", .{pdata.alloc_cpus});
        const alloc_memory = try humanize(allocator, pdata.alloc_memory);
        const idle_memory = try humanize(allocator, pdata.idle_memory);

        if (arg_free) {
            try table.addRow(&.{ pdata.name, idle_cpus, idle_memory });
        } else {
            try table.addRow(
                &.{
                    pdata.name,
                    alloc_cpus,
                    idle_cpus,
                    alloc_memory,
                    idle_memory,
                },
            );
        }

        total_util.alloc_memory += pdata.alloc_memory;
        total_util.idle_memory += pdata.idle_memory;
        total_util.alloc_cpus += pdata.alloc_cpus;
        total_util.idle_cpus += pdata.idle_cpus;
        total_util.total_cpus += pdata.total_cpus;
        total_util.real_memory += pdata.real_memory;
    }

    if (res.args.stats == 0) {
        try table.printstd();
        try stdout.print("\n", .{});
    }

    try show_node_total_util(allocator, total_util);
}

pub fn main() !void {
    slurm.init(null);
    defer slurm.deinit();

    const stdout = std.io.getStdOut().writer();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const params = comptime clap.parseParamsComptime(
        \\-h, --help             Display this help and exit.
        \\-f, --free             Only show how much resources are free.
        \\-n, --nodes            Show node-related utlization. This is the default.
        \\-s, --stats            Only show total Cluster utilization.
        \\-p, --part             Show partition related utization.
    );

    var diag = clap.Diagnostic{};
    var res = clap.parse(clap.Help, &params, clap.parsers.default, .{
        .diagnostic = &diag,
        .allocator = allocator,
    }) catch |err| {
        // Report useful error and exit
        diag.report(std.io.getStdErr().writer(), err) catch {};
        return err;
    };
    defer res.deinit();

    if (res.args.help != 0)
        return clap.help(
            std.io.getStdErr().writer(),
            clap.Help,
            &params,
            .{ .spacing_between_parameters = 0 },
        );

    const part_arg = res.args.part == 0;
    const node_arg = part_arg or res.args.nodes != 0;

    if (node_arg) try show_nodes(allocator, res, stdout);
    if (part_arg) show_parts();
}
