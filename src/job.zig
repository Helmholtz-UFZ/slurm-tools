const std = @import("std");
const slurm = @import("slurm");
const pt = @import("prettytable");
const yazap = @import("yazap");
const allocPrint = std.fmt.allocPrint;
const allocPrintZ = std.fmt.allocPrintZ;
const Allocator = std.mem.Allocator;
const ArgMatches = yazap.ArgMatches;
const slurm_allocator = slurm.slurm_allocator;

var args: Args = .{};

pub const StepWithStats = struct {
    step: *slurm.Step,
    stats: slurm.Step.Statistics = .{},
};

pub const JobWithStats = struct {
    job: *slurm.Job,
    user_name: [:0]const u8,
    account: [:0]const u8,
    partition: [:0]const u8,
    nodes: [:0]const u8,
    run_time: std.posix.time_t,
    steps: std.ArrayList(StepWithStats),
    stats: slurm.Job.Statistics = .{},
};

pub const StatResponse = struct {
    data: std.AutoHashMap(u32, JobWithStats),
    count: u64,
    _job_data: *slurm.Job.LoadResponse,
    _step_data: *slurm.Step.LoadResponse,

    pub fn deinit(self: *StatResponse) void {
        self._job_data.deinit();
        self._step_data.deinit();
    }
};

fn getUserName(allocator: Allocator, job: *slurm.Job) ![:0]const u8 {
    if (job.user_name) |uname| {
        return std.mem.span(uname);
    }

    const passwd_info = std.c.getpwuid(job.user_id);
    if (passwd_info) |pwd| {
        if (pwd.name) |name| {
            const pwd_name = std.mem.span(name);
            return try slurm_allocator.dupeZ(u8, pwd_name);
        }
    }

    return try allocPrintZ(allocator, "{d}", .{job.user_id});
}

pub fn stat(allocator: Allocator) !StatResponse {
    var jobs = try slurm.loadJobs();
    var job_iter = jobs.iter();

    const steps = try slurm.step.loadAll();
    var step_iter = steps.iter();

    var stat_resp: StatResponse = .{
        .data = .init(allocator),
        .count = 0,
        ._job_data = jobs,
        ._step_data = steps,
    };

    outer: while (job_iter.next()) |job| {
        const user_name = try getUserName(allocator, job);
        const account = slurm.parseCStrZ(job.account) orelse "N/A";
        const partition = slurm.parseCStrZ(job.partition) orelse "N/A";
        const nodes = slurm.parseCStrZ(job.nodes) orelse "N/A";
        const run_time = job.runTime();

        // Start to filter anything that the user doesn't want to see.
        for (args.jobs.items, 0..) |arg_job, idx| {
            if (arg_job == job.job_id) break;
            if (idx == args.jobs.items.len - 1) continue :outer;
        }

        for (args.users.items, 0..) |arg_user, idx| {
            if (std.mem.eql(u8, arg_user, user_name)) break;
            if (idx == args.users.items.len - 1) continue :outer;
        }

        for (args.accounts.items, 0..) |arg_account, idx| {
            if (std.mem.eql(u8, arg_account, account)) break;
            if (idx == args.accounts.items.len - 1) continue :outer;
        }

        for (args.partitions.items, 0..) |arg_partition, idx| {
            if (std.mem.eql(u8, arg_partition, partition)) break;
            if (idx == args.partitions.items.len - 1) continue :outer;
        }

        for (args.nodes.items, 0..) |arg_nodes, idx| {
            // TODO: Needs Slurm's HostList API
            _ = arg_nodes;
            _ = idx;
        }

        if (run_time <= args.min_runtime * 60) continue;
        if (job.num_cpus < args.min_cpus) continue;
        if (job.num_cpus > args.max_cpus) continue;

        const res = try stat_resp.data.getOrPut(job.job_id);
        if (!res.found_existing) {
            res.value_ptr.* = .{
                .job = job,
                .steps = .init(allocator),
                .stats = .{},
                .user_name = user_name,
                .account = account,
                .partition = partition,
                .nodes = nodes,
                .run_time = run_time,
            };
            stat_resp.count += 1;
        }
    }

    while (step_iter.next()) |step| {
        const job_id = step.step_id.job_id;
        const stats = try slurm.step.stat(allocator, step);

        if (stat_resp.data.getPtr(job_id)) |v| {
            try v.steps.append(.{
                .step = step,
                .stats = stats,
            });

            v.stats.consumed_energy += stats.consumed_energy;
            v.stats.disk_read += stats.avg_disk_read;
            v.stats.disk_write += stats.avg_disk_write;
            v.stats.page_faults += stats.avg_page_faults;
            v.stats.total_cpu_time += stats.total_cpu_time;
            v.stats.user_cpu_time += stats.user_cpu_time;
            v.stats.system_cpu_time += stats.system_cpu_time;

            if (stats.max_resident_memory > v.stats.resident_memory) {
                v.stats.resident_memory = stats.max_resident_memory;
            }

            if (stats.max_virtual_memory > v.stats.virtual_memory) {
                v.stats.virtual_memory = stats.max_virtual_memory;
            }
        }
    }
    return stat_resp;
}

pub const CPUEfficiency = struct {
    status: Status,
    percent: f64,

    pub const Status = enum {
        ok,
        over,
        critical,
        under,
    };
};

fn humanize(allocator: Allocator, val: u64, from: ?[]const u8) ![]const u8 {
    const units = [_][]const u8{ "B", "K", "M", "G", "T", "P", "E", "Z" };
    const start_from = if (from) |f| f else "B";
    var start = false;
    var fl: f64 = @floatFromInt(val);

    for (units) |unit| {
        if (std.mem.eql(u8, unit, start_from)) start = true;
        if (!start) continue;

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

pub fn getCpuEfficiencyState(
    total_cpu_time: u64,
    elapsed_cpu_time: u64,
    alloc_cpus: u32,
    run_time: std.posix.time_t,
) CPUEfficiency {
    var state: CPUEfficiency.Status = .ok;

    if (total_cpu_time > ((elapsed_cpu_time * args.util_upper) / 100)) {
        state = .over;
    } else if (alloc_cpus > 1 and total_cpu_time < run_time) {
        state = .critical;
    } else if (total_cpu_time < ((elapsed_cpu_time * args.util_lower) / 100)) {
        state = .under;
    }

    return .{
        .status = state,
        .percent = to_percent(f64, total_cpu_time, elapsed_cpu_time),
    };
}

pub fn processJobs(allocator: Allocator) !void {
    var table = pt.Table.init(allocator);
    defer table.deinit();

    try table.setTitle(&.{
        "JobID",
        "Status",
        "Nodes",
        "User",
        "Account",
        "Elapsed",
        "Remaining",
        "NumCPUs",
        "CPU%",
        "CPU Optimum",
        "Memory Peak",
    });

    var stat_resp = try stat(allocator);
    var iter = stat_resp.data.valueIterator();

    while (iter.next()) |item| {
        const job = item.job;

        if (job.state.base != .running) continue;

        const user_name = item.user_name;
        const account = item.account;
        const nodes = item.nodes;
        const run_time = item.run_time;
        const time_limit = if (job.time_limit != slurm.common.Infinite.u32) job.time_limit else 0;
        const elapsed_cpu_time: u64 = @intCast(run_time * job.num_cpus);

        const job_eff = getCpuEfficiencyState(
            item.stats.total_cpu_time,
            elapsed_cpu_time,
            job.num_cpus,
            run_time,
        );

        if (job_eff.status == .ok and !args.with_ok) continue;

        try table.addRow(&[_][]const u8{
            try allocPrint(allocator, "{d}", .{job.job_id}),
            @tagName(job_eff.status),
            nodes,
            user_name,
            account,
            try allocPrint(allocator, "{d}", .{run_time}),
            try allocPrint(allocator, "{d}", .{(time_limit * 60) - run_time}),
            try allocPrint(allocator, "{d}", .{job.num_cpus}),
            try allocPrint(allocator, "{d:.2}%", .{job_eff.percent}),
            try allocPrint(allocator, "{d}", .{elapsed_cpu_time}),
            try humanize(allocator, item.stats.resident_memory, "B"),
        });

        if (args.all) {
            for (item.steps.items) |step_stat| {
                const step = step_stat.step;
                const stats = step_stat.stats;

                const num_cpus = if (step.num_cpus > 0) step.num_cpus else 1;

                const step_eff = getCpuEfficiencyState(
                    stats.total_cpu_time,
                    stats.elapsed_cpu_time,
                    num_cpus,
                    step.run_time,
                );

                const step_time_limit = if (step.time_limit != slurm.common.Infinite.u32) step.time_limit else job.time_limit;

                try table.addRow(&[_][]const u8{
                    try allocPrint(allocator, "  {d}.{d}", .{ job.job_id, step.step_id.step_id }),
                    @tagName(step_eff.status),
                    if (step.nodes) |n| std.mem.span(n) else "N/A",
                    user_name,
                    account,
                    try allocPrint(allocator, "{d}", .{step.run_time}),
                    try allocPrint(allocator, "{d}", .{(step_time_limit * 60) - step.run_time}),
                    try allocPrint(allocator, "{d}", .{num_cpus}),
                    try allocPrint(allocator, "{d:.2}%", .{step_eff.percent}),
                    try allocPrint(allocator, "{d}", .{stats.elapsed_cpu_time}),
                    try humanize(allocator, stats.avg_resident_memory, "B"),
                });
            }
        }
    }
    if (table.len() > 0) try table.printstd();
}

pub const Args = struct {
    all: bool = false,
    with_ok: bool = false,
    util_lower: u8 = 80,
    util_upper: u8 = 101,
    jobs: std.ArrayListUnmanaged(u32) = .empty,
    users: std.ArrayListUnmanaged([]const u8) = .empty,
    nodes: std.ArrayListUnmanaged([]const u8) = .empty,
    accounts: std.ArrayListUnmanaged([]const u8) = .empty,
    partitions: std.ArrayListUnmanaged([]const u8) = .empty,
    unit: []const u8 = "G",
    min_cpus: u32 = 0,
    max_cpus: u32 = (1 << 32) - 1,
    min_runtime: u32 = 5,

    pub fn parseDelimiterOption(
        T: type,
        buf: *std.ArrayListUnmanaged(T),
        allocator: Allocator,
        delim: u8,
        input: ?[]const u8,
    ) !void {
        if (input == null) return;

        var splitted = std.mem.splitScalar(u8, input.?, delim);
        while (splitted.next()) |job_id| {
            const item = switch (T) {
                []const u8 => job_id,
                else => try std.fmt.parseInt(T, job_id, 10),
            };

            try buf.append(allocator, item);
        }
    }

    pub fn parseDelimiterOptionString(
        buf: *std.ArrayListUnmanaged([]const u8),
        allocator: Allocator,
        delim: u8,
        input: ?[]const u8,
    ) !void {
        try Args.parseDelimiterOption([]const u8, buf, allocator, delim, input);
    }
};

pub fn parseArgs(matches: ArgMatches, allocator: Allocator) !Args {
    args = .{
        .all = matches.containsArg("all"),
        .with_ok = matches.containsArg("with-ok"),
    };

    try Args.parseDelimiterOption(
        u32,
        &args.jobs,
        allocator,
        ',',
        matches.getSingleValue("jobs"),
    );

    try Args.parseDelimiterOptionString(
        &args.users,
        allocator,
        ',',
        matches.getSingleValue("users"),
    );

    try Args.parseDelimiterOptionString(
        &args.nodes,
        allocator,
        ',',
        matches.getSingleValue("nodes"),
    );

    try Args.parseDelimiterOptionString(
        &args.accounts,
        allocator,
        ',',
        matches.getSingleValue("accounts"),
    );

    try Args.parseDelimiterOptionString(
        &args.partitions,
        allocator,
        ',',
        matches.getSingleValue("partitions"),
    );

    if (matches.getSingleValue("unit")) |unit| {
        args.unit = unit;
    }

    if (matches.getSingleValue("underutil-threshold")) |under| {
        args.util_lower = try std.fmt.parseInt(u8, under, 10);
    }

    if (matches.getSingleValue("overutil-threshold")) |over| {
        args.util_upper = try std.fmt.parseInt(u8, over, 10);
    }

    if (matches.getSingleValue("min-cpus")) |min_cpus| {
        args.min_cpus = try std.fmt.parseInt(u32, min_cpus, 10);
    }

    if (matches.getSingleValue("max-cpus")) |max_cpus| {
        args.max_cpus = try std.fmt.parseInt(u32, max_cpus, 10);
    }

    if (matches.getSingleValue("min-runtime")) |min_runtime| {
        args.min_runtime = try std.fmt.parseInt(u32, min_runtime, 10);
    }

    return args;
}

pub fn run(allocator: Allocator, matches: ArgMatches) !void {
    slurm.init(null);
    defer slurm.deinit();

    if (matches.subcommandMatches("stat")) |stats_args| {
        args = try parseArgs(stats_args, allocator);
        try processJobs(allocator);
    }
}
