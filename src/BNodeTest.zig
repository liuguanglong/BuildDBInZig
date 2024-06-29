const std = @import("std");
const util = @import("Util.zig");
const node = @import("BNode.zig");

const print = std.debug.print;

test "header test" {
    std.debug.print("header test\n", .{});

    var data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
    var nodeA = node.BNode.initSigleCapacity(&data);

    nodeA.setHeader(node.BNODE_NODE, 20);
    try std.testing.expectEqual(node.BNODE_NODE, nodeA.btype());
    try std.testing.expectEqual(20, nodeA.nkeys());

    const ptr: u64 = 0x123456789ABCDEF0;
    nodeA.setPtr(19, ptr);
    //nodeA.print();
    try std.testing.expectEqual(ptr, nodeA.getPtr(19));
}

test "dynamic create test" {
    std.debug.print("dynamic create test\n", .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("TEST FAIL");
    }

    var ptrData = allocator.alloc(u8, node.BTREE_PAGE_SIZE) catch {
        unreachable;
    };
    var newnode = allocator.create(node.BNode) catch {
        unreachable;
    };

    newnode.* = node.BNode{ .data = @unionInit(node.BNodeData, "Single", ptrData[0..node.BTREE_PAGE_SIZE]) };
    newnode.setHeader(node.BNODE_NODE, 20);
    try std.testing.expectEqual(node.BNODE_NODE, newnode.btype());
    try std.testing.expectEqual(20, newnode.nkeys());

    const ptr1 = newnode.data.Single;
    allocator.destroy(newnode);
    allocator.free(ptr1);
}

test "ptr test" {
    std.debug.print("ptr test\n", .{});
    var data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
    var nodeA = node.BNode.initSigleCapacity(&data);

    nodeA.setHeader(node.BNODE_NODE, 2);
    const ptr: u64 = 0x123456789ABCDEF0;
    nodeA.setPtr(0, ptr);
    nodeA.setPtr(1, ptr);
    //nodeA.print();
    try std.testing.expectEqual(ptr, nodeA.getPtr(0));
}

test "offset test" {
    std.debug.print("offset test\n", .{});
    var data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
    var nodeA = node.BNode.initSigleCapacity(&data);

    nodeA.setHeader(node.BNODE_NODE, 2);
    const offset: u16 = 0x1234;
    nodeA.setOffSet(1, offset);
    nodeA.setOffSet(2, offset);

    //nodeA.print();
    try std.testing.expectEqual(0x1234, nodeA.getOffSet(1));
}

test "nodeAppendKV test" {
    std.debug.print("nodeAppendKV test\n", .{});
    var data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
    var root = node.BNode.initSigleCapacity(&data);

    root.setHeader(node.BNODE_LEAF, 3);
    root.nodeAppendKV(0, 0, "", "");
    root.nodeAppendKV(1, 0, "1111", "abcdefg");
    root.nodeAppendKV(2, 0, "2222", "eeeeeee");
    //root.print();

    std.debug.print("offset 1:{d}\n", .{root.getOffSet(1)});
    std.debug.print("kvpos 1:{d}\n", .{root.kvPos(1)});

    const key = root.getKey(1);
    const val = root.getValue(1);

    std.debug.print("Key1:{s}\n", .{key});
    std.debug.print("val:{s}\n", .{val});

    std.debug.print("offset 2:{d}\n", .{root.getOffSet(1)});
    std.debug.print("kvpos 2:{d}\n", .{root.kvPos(1)});

    const key1 = root.getKey(2);
    const val1 = root.getValue(2);

    std.debug.print("Key1:{s}\n", .{key1});
    std.debug.print("val:{s}\n", .{val1});
}

test "leafinsert test" {
    std.debug.print("leafinset test\n", .{});
    var data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
    var root = node.BNode.initSigleCapacity(&data);

    root.setHeader(node.BNODE_LEAF, 2);
    root.nodeAppendKV(0, 0, "", "");
    root.nodeAppendKV(1, 0, "1111", "abcdefg");
    //root.print();

    var data1 = [_]u8{0} ** node.BTREE_PAGE_SIZE;
    var newKey = node.BNode.initSigleCapacity(&data1);
    newKey.leafInsert(&root, 1, "2222", "eeeeee");
    //newKey.print();

    std.debug.print("offset 1:{d}\n", .{newKey.getOffSet(1)});
    std.debug.print("kvpos 1:{d}\n", .{newKey.kvPos(1)});

    const key = newKey.getKey(1);
    const val = newKey.getValue(1);

    std.debug.print("Key1:{s}\n", .{key});
    std.debug.print("val:{s}\n", .{val});
}

test "NodeSplit2 test" {
    std.debug.print("NodeSplit2 test\n", .{});
    var data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
    var root = node.BNode.initSigleCapacity(&data);

    root.setHeader(node.BNODE_LEAF, 2);
    root.nodeAppendKV(0, 0, "", "");
    root.nodeAppendKV(1, 0, "1", "a" ** 3800);

    var data1 = [_]u8{0} ** (2 * node.BTREE_PAGE_SIZE);
    var newKey = node.BNode.initDoubleCapacityNode(&data1);
    newKey.leafInsert(&root, 2, "2", "b" ** 3800);
    //newKey.print();

    const idx = newKey.findSplitIdx();
    try std.testing.expectEqual(@as(u16, 2), idx);

    var dataleft = [_]u8{0} ** node.BTREE_PAGE_SIZE;
    var dataright = [_]u8{0} ** node.BTREE_PAGE_SIZE;
    var left = node.BNode.initSigleCapacity(&dataleft);
    var right = node.BNode.initSigleCapacity(&dataright);

    left.nodeSplit2(&right, &newKey);

    //left.print();
    //right.print();
}

test "nodeSplit3 test" {
    std.debug.print("nodeSplit3 test\n", .{});
    var data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
    var root = node.BNode.initSigleCapacity(&data);

    root.setHeader(node.BNODE_LEAF, 2);
    root.nodeAppendKV(0, 0, "", "");
    root.nodeAppendKV(1, 0, "1111", "a" ** 2500);

    var data1 = [_]u8{0} ** (2 * node.BTREE_PAGE_SIZE);
    var key1 = node.BNode.initDoubleCapacityNode(&data1);

    key1.leafInsert(&root, 2, "2222", "b" ** 2500);

    var data2 = [_]u8{0} ** (2 * node.BTREE_PAGE_SIZE);
    var key2 = node.BNode.(&data2);
    key2.leafInsert(&key1, 2, "3333", "c" ** 2500);
    //key2.print();

    var node1 = [_]u8{0} ** node.BTREE_PAGE_SIZE;
    var node2 = [_]u8{0} ** node.BTREE_PAGE_SIZE;
    var node3 = [_]u8{0} ** node.BTREE_PAGE_SIZE;
    var n1 = node.BNode.initSigleCapacity(&node1);
    var n2 = node.BNode.initSigleCapacity(&node2);
    var n3 = node.BNode.initSigleCapacity(&node3);

    var subNodes = node.SplitNodes{ .Count = 0, .Nodes = .{ &n1, &n2, &n3 } };
    key2.nodeSplit3(&subNodes);
    std.debug.print("Count {d} test\n", .{subNodes.Count});

    subNodes.Nodes[0].print();
    subNodes.Nodes[1].print();
    subNodes.Nodes[2].print();
}

test "leafupdate test" {
    std.debug.print("leafupdate test\n", .{});
    var data1 = [_]u8{0} ** node.BTREE_PAGE_SIZE;
    var data2 = [_]u8{0} ** node.BTREE_PAGE_SIZE;

    var root = node.BNode.initSigleCapacity(&data1);

    root.setHeader(node.BNODE_LEAF, 2);
    root.nodeAppendKV(0, 0, "", "");
    root.nodeAppendKV(1, 0, "1111", "abcdefg");
    //root.print();

    var newKey = node.BNode.initSigleCapacity(&data2);
    newKey.setHeader(node.BNODE_LEAF, 2);

    newKey.leafUpdate(&root, 1, "1111", "fffffff");
    //newKey.print();
}

// test "BNode Type" {
//     var n1 = node.BNode.init(node.Type.Single);
//     n1.print();

//     var n2 = node.BNode.init(node.Type.Double);
//     n2.print();
// }
