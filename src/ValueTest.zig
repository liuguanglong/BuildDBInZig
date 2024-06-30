const std = @import("std");
const value = @import("Value.zig");
const table = @import("Table.zig");

pub const testTable = &table.TableDef{
    .Prefix = 3,
    .Name = "person",
    .Types = &.{ value.ValueType.INT16, value.ValueType.BYTES, value.ValueType.BYTES, value.ValueType.INT16, value.ValueType.BOOL },
    .Cols = &.{ "id", "name", "address", "age", "married" },
    .PKeys = 0,
};

test "Value Seralize" {
    const str = "testtest";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("TEST FAIL");
    }

    const array = try allocator.alloc(u8, 8);
    @memcpy(array, str);
    defer allocator.free(array);

    const s = value.Value{ .BYTES = array };
    std.debug.print("Conent: {s}", .{s.BYTES});
}

test "Record Test" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("TEST FAIL");
    }
    var r = try table.Record.init(allocator, testTable);
    defer r.deinit();

    try r.Set([]const u8, "name", "liugl");
    try r.Set(i16, "age", 36);
    try r.Set([]const u8, "address", "Pointe-Claire");
    try r.Set(bool, "married", true);
    std.debug.print("Record Conent: {} \n", .{r});

    var list = std.ArrayList(u8).init(allocator);
    defer list.deinit();

    const bCheck = testTable.checkRecord(&r);
    std.debug.print("Check Record: {any} \n", .{bCheck});

    try r.encodeValues(&list);
    std.debug.print("Conent: {s} \n", .{list.items});

    const v = try r.Get("address");
    std.debug.print("Address: {s}", .{v.?.BYTES});

    var key = std.ArrayList(u8).init(allocator);
    defer key.deinit();

    try r.Set(i16, "id", 20);
    try r.encodeKey(32, &key);
    std.debug.print("Key Conent: {s} \n", .{key.items});
}
