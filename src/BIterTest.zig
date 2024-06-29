const std = @import("std");
const node = @import("BNode.zig");
const biter = @import("BIter.zig");
const kvcontext = @import("KVContext.zig");
const btree = @import("BTree.zig");

test "Test BITer with OP_CMP" {
    std.debug.print("Test BITer with OP_CMP\n", .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("TEST FAIL");
    }

    const context = try kvcontext.createMemoryContext(allocator);
    defer allocator.destroy(context);
    try context.open();
    defer {
        context.deinit();
    }

    var tree = try allocator.create(btree.BTree);
    defer allocator.destroy(tree);
    try tree.init1(allocator, context);
    defer tree.deinit();

    try tree.Set("5", "222", 0);
    try tree.Set("7", "444", 0);
    try tree.Set("1", "111", 0);
    try tree.Set("3", "333", 0);

    //tree.print();

    var it = try tree.Seek("3", biter.OP_CMP.CMP_LT);
    defer {
        it.deinit();
    }
    const ret = it.Deref();
    std.debug.print("Less Then => Key:{s} Value:{s} \n", .{ ret.key, ret.val });

    var it1 = try tree.Seek("3", biter.OP_CMP.CMP_LE);
    defer {
        it1.deinit();
    }
    const ret1 = it1.Deref();
    std.debug.print("Less and Equal => Key:{s} Value:{s} \n", .{ ret1.key, ret1.val });

    var it2 = try tree.Seek("3", biter.OP_CMP.CMP_GT);
    defer {
        it2.deinit();
    }
    const ret2 = it2.Deref();
    std.debug.print("Large Than => Key:{s} Value:{s} \n", .{ ret2.key, ret2.val });

    var it3 = try tree.Seek("3", biter.OP_CMP.CMP_GE);
    defer {
        it3.deinit();
    }
    const ret3 = it3.Deref();
    std.debug.print("Large and Equal => Key:{s} Value:{s} \n", .{ ret3.key, ret3.val });
}
test "Test BIter With SeekLE" {
    std.debug.print("Test BIter With SeekLE\n", .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("TEST FAIL");
    }

    const context = try kvcontext.createMemoryContext(allocator);
    //defer allocator.destroy(context);
    try context.open();
    defer {
        context.deinit();
        allocator.destroy(context);
    }

    var tree = try allocator.create(btree.BTree);
    defer allocator.destroy(tree);
    try tree.init1(allocator, context);
    defer tree.deinit();

    try tree.Set("2", "222", 0);
    try tree.Set("4", "444", 0);
    try tree.Set("1", "111", 0);
    try tree.Set("3", "333", 0);

    //tree.print();

    var it = try tree.SeekLE("2");
    defer {
        it.deinit();
    }

    var ret = it.Deref();
    std.debug.print("Key:{s} Value:{s} \n", .{ ret.key, ret.val });

    if (it.Prev()) {
        ret = it.Deref();
        std.debug.print("Key:{s} Value:{s} \n", .{ ret.key, ret.val });
    }

    if (it.Prev()) {
        ret = it.Deref();
        std.debug.print("Key:{s} Value:{s} \n", .{ ret.key, ret.val });
    }

    if (it.Prev()) {
        ret = it.Deref();
        std.debug.print("Key:{s} Value:{s} \n", .{ ret.key, ret.val });
    }

    if (it.Next()) {
        ret = it.Deref();
        std.debug.print("Key:{s} Value:{s} \n", .{ ret.key, ret.val });
    }

    if (it.Next()) {
        ret = it.Deref();
        std.debug.print("Key:{s} Value:{s} \n", .{ ret.key, ret.val });
    }

    if (it.Next()) {
        ret = it.Deref();
        std.debug.print("Key:{s} Value:{s} \n", .{ ret.key, ret.val });
    }

    if (it.Next()) {
        ret = it.Deref();
        std.debug.print("Key:{s} Value:{s} \n", .{ ret.key, ret.val });
    }
}
