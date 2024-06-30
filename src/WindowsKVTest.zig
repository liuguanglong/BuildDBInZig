const std = @import("std");
const db = @import("KVContext.zig");
const btree = @import("BTree.zig");
const windowskv = @import("WindowsKV.zig");

test "Windows DataBase Test" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("TEST FAIL");
    }

    const fileName: [:0]const u8 = "c:/temp/winkv.data";
    var instance = try allocator.create(windowskv.WindowsKV);
    defer allocator.destroy(instance);

    try instance.init(allocator, fileName, 20);
    defer instance.deinit();

    try instance.Set("2", "222", btree.MODE_UPSERT);
    try instance.Set("4", "444", btree.MODE_UPSERT);
    try instance.Set("1", "111", btree.MODE_UPSERT);
    try instance.Set("3", "333", btree.MODE_UPSERT);

    _ = try instance.Delete("4");

    instance.print();
    const ret = instance.Get("3");
    if (ret) |v|
        std.debug.print("Result: {s} \n", .{v});
}
