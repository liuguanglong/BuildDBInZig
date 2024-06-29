const std = @import("std");
const node = @import("BNode.zig");
const kvcontext = @import("KVContext.zig");
const mc = @import("MemoryContext.zig");

test "open MemoryContext" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("TEST FAIL");
    }

    var context = try kvcontext.createMemoryContext(allocator);
    defer allocator.destroy(context);

    try context.open();
    defer {
        context.close() catch {
            std.debug.print("DB Closed Execption", .{});
        };
        context.deinit();
    }

    var data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
    var nodeA = node.BNode.initSigleCapacity(&data);
    nodeA.setHeader(node.BNODE_NODE, 2);
    nodeA.nodeAppendKV(0, 0, "", "");
    nodeA.nodeAppendKV(1, 0, "1111", "abcdefg");

    std.debug.print("Orgin Node\n", .{});
    //nodeA.print();

    const ptr = try context.new(&nodeA);
    var pNode = try context.get(ptr);
    std.debug.print("Get Node\n", .{});
    //pNode.print();

    //try std.testing.expectEqual(&nodeA, pNode);
    std.debug.assert(std.mem.eql(u8, nodeA.getdata()[0..node.BTREE_PAGE_SIZE], pNode.getdata()[0..node.BTREE_PAGE_SIZE]));
}
