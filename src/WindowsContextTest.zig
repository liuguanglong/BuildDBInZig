const std = @import("std");
const node = @import("BNode.zig");
const kvcontext = @import("KVContext.zig");
const wincontext = @import("WindowsContext.zig");

test "open test" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("TEST FAIL");
    }

    const filename: [:0]const u8 = "c:/temp/windb.data";
    var context = try kvcontext.createWindowsContext(allocator, filename, 20);
    defer allocator.destroy(context);

    try context.open();
    defer {
        context.close() catch {
            std.debug.print("DB Closed Execption", .{});
        };
        context.deinit();
    }

    var nodeA = node.BNode.init(node.Type.Single);
    nodeA.setHeader(node.BNODE_NODE, 2);
    nodeA.nodeAppendKV(0, 0, "", "");
    nodeA.nodeAppendKV(1, 0, "2222", "abcdefg");

    std.debug.print("Orgin Node\n", .{});
    nodeA.print();

    const ptr = try context.new(nodeA);
    try context.save();

    var pNode = try context.get(ptr);
    std.debug.print("Get Node\n", .{});
    pNode.print();

    std.debug.assert(std.mem.eql(u8, nodeA.getdata()[0..node.BTREE_PAGE_SIZE], pNode.getdata()[0..node.BTREE_PAGE_SIZE]));
}
