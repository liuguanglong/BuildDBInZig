const std = @import("std");
const util = @import("Util.zig");
const value = @import("Value.zig");
const table = @import("Table.zig");

pub const prefrixes = [_]u32{};
pub const testTable = &table.TableDef{
    .Prefix = 3,
    .Name = "person",
    .Types = &.{ value.ValueType.BYTES, value.ValueType.BYTES, value.ValueType.BYTES, value.ValueType.INT16, value.ValueType.BOOL },
    .Cols = &.{ "id", "name", "address", "age", "married" },
    .PKeys = 1,
    .Indexes = &.{ &.{ "married", "address" }, &.{"age"} },
    .IndexPrefixes = &prefrixes,
};

test "Table Define Copy" {
    std.debug.print("\nTable Define Copy\n", .{});
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("TEST FAIL");
    }

    const def = try allocator.create(table.TableDef);
    def.* = testTable.*;
    defer allocator.destroy(def);

    std.debug.print("UnMarshal Result:\n {}", .{def});
}

test "Table Define Marshal" {
    std.debug.print("\nTable Define Marshal\n", .{});
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("TEST FAIL");
    }

    var strTableDef = std.ArrayList(u8).init(allocator);
    defer strTableDef.deinit();
    try table.Marshal(allocator, testTable, &strTableDef);

    std.debug.print("Marshal Result:\n {s}\n", .{strTableDef.items});

    const parsed = try std.json.parseFromSlice(
        table.TableDef,
        allocator,
        strTableDef.items,
        .{},
    );
    defer parsed.deinit();

    std.debug.print("UnMarshal Result:\n {s}", .{parsed.value});
}
test "Table Check PKey" {
    std.debug.print("\nTest Check PKey\n", .{});
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("TEST FAIL");
    }

    var r = try table.Record.init(allocator, testTable);
    defer r.deinit();

    try r.Set([]const u8, "id", "20");
    try r.Set([]const u8, "name", "bob");
    try r.Set([]const u8, "address", "Pointe-Claire");
    try r.Set(i16, "age", 36);

    const bCheck = testTable.checkPrimaryKey(&r);
    const bCheckIndexes = testTable.checkIndexes(&r);

    std.debug.print("Primary Key Result: {}\n  Indexes Result:{} \n", .{ bCheck, bCheckIndexes });
}

test "Table Check ReOrder" {
    std.debug.print("\nTest ReOrder\n", .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("TEST FAIL");
    }

    var r = try table.Record.init(allocator, testTable);
    defer r.deinit();

    try r.Set([]const u8, "id", "20");
    try r.Set(i16, "age", 36);
    try r.Set([]const u8, "name", "bob");
    try r.Set([]const u8, "address", "Pointe-Claire");
    try r.Set(bool, "married", true);
    std.debug.print("Before: {} \n", .{r});

    const bCheck = testTable.checkRecord(&r);

    std.debug.print("Result: {}", .{bCheck});
}
