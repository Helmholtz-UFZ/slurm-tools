const std = @import("std");
const slurm = @import("slurm");
const pt = @import("prettytable");
const yazap = @import("yazap");
const allocPrint = std.fmt.allocPrint;
const Allocator = std.mem.Allocator;
const ArgMatches = yazap.ArgMatches;

var args: Args = .{};

pub const StepWithStats = struct {
    step: *slurm.Step,
    stats: slurm.Step.Statistics = .{},
};

pub const JobWithStats = struct {
    job: *slurm.Job,
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

    while (job_iter.next()) |job| {
        const res = try stat_resp.data.getOrPut(job.job_id);
        if (!res.found_existing) {
            res.value_ptr.* = .{
                .job = job,
                .steps = .init(allocator),
                .stats = .{},
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

fn humanize(allocator: Allocator, val: u128) ![]const u8 {
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

        const user_name = slurm.parseCStrZ(job.user_name) orelse "N/A";
        const account = slurm.parseCStrZ(job.account) orelse "N/A";
        const run_time = job.runTime();
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
            if (job.nodes) |n| std.mem.span(n) else "N/A",
            user_name,
            account,
            try allocPrint(allocator, "{d}", .{run_time}),
            try allocPrint(allocator, "{d}", .{(time_limit * 60) - run_time}),
            try allocPrint(allocator, "{d}", .{job.num_cpus}),
            try allocPrint(allocator, "{d:.2}", .{job_eff.percent}),
            try allocPrint(allocator, "{d}", .{elapsed_cpu_time}),
            try allocPrint(allocator, "{d}", .{item.stats.resident_memory}),
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
                    try allocPrint(allocator, "{d:.2}", .{step_eff.percent}),
                    try allocPrint(allocator, "{d}", .{stats.elapsed_cpu_time}),
                    try allocPrint(allocator, "{d}", .{stats.avg_resident_memory}),
                });
            }
        }

        try table.printstd();
    }
}

pub const Args = struct {
    all: bool = false,
    with_ok: bool = false,
    util_lower: u8 = 80,
    util_upper: u8 = 101,
};

pub fn parseArgs(matches: ArgMatches) !Args {
    args = .{
        .with_ok = matches.containsArg("with-ok"),
        .all = matches.containsArg("all"),
    };

    if (matches.getSingleValue("underu")) |under| {
        args.util_lower = try std.fmt.parseInt(u8, under, 10);
    }

    if (matches.getSingleValue("overu")) |over| {
        args.util_upper = try std.fmt.parseInt(u8, over, 10);
    }

    return args;
}

pub fn run(allocator: Allocator, matches: ArgMatches) !void {
    slurm.init(null);
    defer slurm.deinit();

    if (matches.subcommandMatches("stat")) |stats_args| {
        args = try parseArgs(stats_args);
        try processJobs(allocator);
    }
}
