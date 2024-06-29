const std = @import("std");
const node = @import("BNode.zig");
const btree = @import("BTree.zig");
const dbcontext = @import("KVContext.zig");
const memcontext = @import("MemoryContext.zig");

test "test btree with windowkv persist" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("TEST FAIL");
    }

    const filename: [:0]const u8 = "c:/temp/windb.data";
    var context = try dbcontext.createWindowsContext(allocator, filename, 40);
    defer allocator.destroy(context);

    try context.open();
    defer {
        context.close() catch {
            std.debug.print("DB Closed Execption", .{});
        };
        context.deinit();
    }

    var tree = try btree.BTree.init(allocator, context);
    defer tree.deinit();

    try tree.Set("2", "2" ** 2500, btree.MODE_UPSERT);
    try tree.Set("4", "4" ** 2500, btree.MODE_UPSERT);
    try tree.Set("1", "1" ** 2500, btree.MODE_UPSERT);
    try tree.Set("3", "3" ** 2500, btree.MODE_UPSERT);

    _ = try tree.Delete("2");
    _ = try tree.Delete("4");

    tree.print();
}

// test "test btree with memory persist" {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//     defer {
//         const deinit_status = gpa.deinit();
//         if (deinit_status == .leak) @panic("TEST FAIL");
//     }

//     var context = try dbcontext.createMemoryContext(allocator);
//     defer allocator.destroy(context);

//     try context.open();
//     defer {
//         context.close() catch {
//             std.debug.print("DB Closed Execption", .{});
//         };
//         context.deinit();
//     }

//     var tree = try btree.BTree.init(allocator, context);
//     defer tree.deinit();

//     try tree.Set("2", "2" ** 2500, btree.MODE_UPSERT);
//     try tree.Set("4", "4" ** 2500, btree.MODE_UPSERT);
//     try tree.Set("1", "1" ** 2500, btree.MODE_UPSERT);
//     try tree.Set("3", "3" ** 2500, btree.MODE_UPSERT);

//     _ = try tree.Delete("2");
//     _ = try tree.Delete("4");

//     tree.print();
// }
