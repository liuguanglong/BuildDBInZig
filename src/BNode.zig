const std = @import("std");
const util = @import("Util.zig");

pub const BTREE_PAGE_SIZE = 4096;
pub const BTREE_MAX_KEY_SIZE = 1000;
pub const BTREE_MAX_VALUE_SIZE = 3000;

pub const BNODE_NODE: u16 = 1;
pub const BNODE_LEAF: u16 = 2;
pub const BNODE_FREE_LIST: u16 = 3;
pub const HEADER = 4;

pub const SplitNodes = struct {
    Count: u16,
    Nodes: [3]*BNode,
};

pub const Type = enum(u16) {
    Single,
    Double,
};

pub const BNodeData = union(Type) {
    Single: *[BTREE_PAGE_SIZE]u8,
    Double: *[BTREE_PAGE_SIZE * 2]u8,
};

pub const BNode = struct {
    data: BNodeData,

    pub fn initSigleCapacity(data: *[BTREE_PAGE_SIZE]u8) BNode {
        return BNode{ .data = BNodeData{ .Single = data } };
    }

    pub fn initDoubleCapacityNode(data: *[BTREE_PAGE_SIZE * 2]u8) BNode {
        return BNode{ .data = BNodeData{ .Double = data } };
    }

    pub fn getdata(self: *const BNode) [*]u8 {
        switch (self.data) {
            inline Type.Single => return self.data.Single,
            inline Type.Double => return self.data.Double,
        }
    }

    pub fn size(self: *BNode) u16 {
        switch (self.data) {
            inline Type.Single => return BTREE_PAGE_SIZE,
            inline Type.Double => return BTREE_PAGE_SIZE * 2,
        }
    }

    pub fn print(self: *BNode) void {
        //std.debug.print("Node: Type:{d}", .{self.btype()});
        const ptr = self.getdata();
        const len = self.size();

        for (0..len) |i| {
            if (i > 0) {
                std.debug.print(", ", .{});
            }
            if (i % 50 == 0)
                std.debug.print("\n", .{});
            std.debug.print("{x}", .{ptr[i]});
        }
        std.debug.print("\n", .{});
    }

    //header
    pub fn btype(self: *BNode) u16 {
        const ptr = self.getdata();

        const high: u16 = @as(u16, ptr[0]) << 8;
        const low: u16 = @as(u16, ptr[1]);
        const result: u16 = high | low;

        return result;
    }

    pub fn nkeys(self: *const BNode) u16 {
        const ptr = self.getdata();

        const high: u16 = @as(u16, ptr[2]) << 8;
        const low: u16 = @as(u16, ptr[3]);
        const result: u16 = high | low;

        return result;
    }

    pub fn setHeader(self: *BNode, nodetype: u16, keynumber: u16) void {
        const ptr = self.getdata();

        ptr[0] = @intCast(nodetype >> 8);
        ptr[1] = @intCast(nodetype & 0xFF);

        ptr[2] = @intCast(keynumber >> 8);
        ptr[3] = @intCast(keynumber & 0xFF);
    }

    //Pointers
    pub fn setPtr(self: *BNode, idx: u16, value: u64) void {
        const ptr = self.getdata();

        std.debug.assert(idx < self.nkeys());
        const pos = HEADER + 8 * idx;

        ptr[pos + 0] = @intCast((value >> 56) & 0xFF);
        ptr[pos + 1] = @intCast((value >> 48) & 0xFF);
        ptr[pos + 2] = @intCast((value >> 40) & 0xFF);
        ptr[pos + 3] = @intCast((value >> 32) & 0xFF);
        ptr[pos + 4] = @intCast((value >> 24) & 0xFF);
        ptr[pos + 5] = @intCast((value >> 16) & 0xFF);
        ptr[pos + 6] = @intCast((value >> 8) & 0xFF);
        ptr[pos + 7] = @intCast(value & 0xFF);
    }

    pub fn getPtr(self: *BNode, idx: u16) u64 {
        std.debug.assert(idx < self.nkeys());
        const ptr = self.getdata();
        const pos = HEADER + 8 * idx;

        var number: u64 = 0;
        number |= @as(u64, ptr[pos + 0]) << 56;
        number |= @as(u64, ptr[pos + 1]) << 48;
        number |= @as(u64, ptr[pos + 2]) << 40;
        number |= @as(u64, ptr[pos + 3]) << 32;
        number |= @as(u64, ptr[pos + 4]) << 24;
        number |= @as(u64, ptr[pos + 5]) << 16;
        number |= @as(u64, ptr[pos + 6]) << 8;
        number |= @as(u64, ptr[pos + 7]);

        return number;
    }

    //OffSet list
    pub fn offsetPos(self: *const BNode, idx: u16) u16 {
        std.debug.assert(1 <= idx and idx <= self.nkeys());
        return HEADER + 8 * self.nkeys() + 2 * (idx - 1);
    }

    pub fn getOffSet(self: *const BNode, idx: u16) u16 {
        if (idx == 0)
            return 0;
        const ptr = self.getdata();

        const pos = offsetPos(self, idx);
        const high: u16 = @as(u16, ptr[pos]) << 8;
        const low: u16 = @as(u16, ptr[pos + 1]);
        const result: u16 = high | low;

        return result;
    }

    pub fn setOffSet(self: *BNode, idx: u16, offSet: u16) void {
        const ptr = self.getdata();
        const pos = offsetPos(self, idx);
        ptr[pos] = @intCast(offSet >> 8);
        ptr[pos + 1] = @intCast(offSet & 0xFF);
    }

    //key-values
    pub fn kvPos(self: *const BNode, idx: u16) u16 {
        std.debug.assert(idx <= self.nkeys());
        return HEADER + 8 * self.nkeys() + 2 * self.nkeys() + self.getOffSet(idx);
    }

    pub fn getKey(self: *BNode, idx: u16) []u8 {
        std.debug.assert(idx <= self.nkeys());
        const ptr = self.getdata();
        const pos = self.kvPos(idx);

        const high: u16 = @as(u16, ptr[pos]) << 8;
        const low: u16 = @as(u16, ptr[pos + 1]);
        const klen: u16 = high | low;

        return ptr[pos + 4 ..][0..klen];
    }

    pub fn getValue(self: *BNode, idx: u16) []u8 {
        std.debug.assert(idx <= self.nkeys());
        const ptr = self.getdata();
        const pos = self.kvPos(idx);

        const high: u16 = @as(u16, ptr[pos]) << 8;
        const low: u16 = @as(u16, ptr[pos + 1]);
        const klen: u16 = high | low;

        const high1: u16 = @as(u16, ptr[pos + 2]) << 8;
        const low1: u16 = @as(u16, ptr[pos + 3]);
        const vlen: u16 = high1 | low1;

        return ptr[pos + 4 + klen ..][0..vlen];
    }

    //node size in bytes
    pub fn nbytes(self: *const BNode) u16 {
        return self.kvPos(self.nkeys());
    }

    // returns the first kid node whose range intersects the key. (kid[i] <= key)
    pub fn nodeLookupLE(self: *BNode, key: []const u8) u16 {
        const number = self.nkeys();
        var found: u16 = 0;
        for (1..number) |i| {
            const idx: u16 = @intCast(i);
            const comp = util.compareArrays(self.getKey(idx), key);
            if (comp <= 0) {
                found = idx;
            }
            if (comp >= 0) {
                break;
            }
        }
        return found;
    }

    pub fn nodeAppendKV(self: *BNode, idx: u16, ptr: u64, key: []const u8, val: []const u8) void {
        //ptrs
        self.setPtr(idx, ptr);
        //kvs
        const pos = self.kvPos(idx);
        const ptrNode = self.getdata();

        ptrNode[pos + 0] = @intCast(key.len >> 8);
        ptrNode[pos + 1] = @intCast(key.len & 0xFF);

        ptrNode[pos + 2] = @intCast(val.len >> 8);
        ptrNode[pos + 3] = @intCast(val.len & 0xFF);

        std.mem.copyBackwards(u8, ptrNode[pos + 4 ..][0..key.len], key);
        std.mem.copyBackwards(u8, ptrNode[pos + 4 + key.len ..][0..val.len], val);

        const len: u16 = @intCast(key.len + val.len);
        self.setOffSet(idx + 1, self.getOffSet(idx) + 4 + len);
    }

    pub fn nodeAppendRange(self: *BNode, old: *BNode, dstNew: u16, srcOld: u16, number: u16) void {
        std.debug.assert(srcOld + number <= old.nkeys());
        std.debug.assert(dstNew + number <= self.nkeys());
        const ptrSelf = self.getdata();
        const ptrOld = old.getdata();

        if (number == 0) {
            return;
        }

        //Copy Pointers
        for (0..number) |i| {
            const idx: u16 = @intCast(i);
            self.setPtr(dstNew + idx, old.getPtr(srcOld + idx));
        }

        //Copy Offsets
        const dstBegin = self.getOffSet(dstNew);
        const srcBegin = old.getOffSet(srcOld);
        for (1..number + 1) |i| //Range [1..n]
        {
            const idx1: u16 = @intCast(i);

            const offset = old.getOffSet(srcOld + idx1) - srcBegin + dstBegin;
            self.setOffSet(dstNew + idx1, offset);
        }

        //Copy kvs
        const begin = old.kvPos(srcOld);
        const end = old.kvPos(srcOld + number);
        const len: u16 = @intCast(end - begin);
        std.mem.copyBackwards(u8, ptrSelf[self.kvPos(dstNew)..][0..len], ptrOld[begin..end]);
    }

    //Add new key to a leaf node
    pub fn leafInsert(self: *BNode, old: *BNode, idx: u16, key: []const u8, val: []const u8) void {
        self.setHeader(BNODE_LEAF, old.nkeys() + 1);
        self.nodeAppendRange(old, 0, 0, idx);
        self.nodeAppendKV(idx, 0, key, val);
        self.nodeAppendRange(old, idx + 1, idx, old.nkeys() - idx);
    }

    //update a leaf node
    pub fn leafUpdate(self: *BNode, old: *BNode, idx: u16, key: []const u8, val: []const u8) void {
        self.setHeader(BNODE_LEAF, old.nkeys());
        self.nodeAppendRange(old, 0, 0, idx);
        self.nodeAppendKV(idx, 0, key, val);
        self.nodeAppendRange(old, idx + 1, idx + 1, old.nkeys() - idx - 1);
    }

    // remove a key from a leaf node
    pub fn leafDelete(self: *BNode, old: *BNode, idx: u16) void {
        self.setHeader(BNODE_LEAF, old.nkeys() - 1);
        self.nodeAppendRange(old, 0, 0, idx);
        self.nodeAppendRange(old, idx, idx + 1, old.nkeys() - (idx + 1));
    }

    //find SplitIdx
    //the second node always fits on a page.
    pub fn findSplitIdx(self: *BNode) u16 {
        const number = self.nkeys();

        const lastPos = self.kvPos(number);
        std.debug.print("Key Number:{d} Last Pos:{d}\n", .{ number, lastPos });

        var find: u16 = number - 1;
        var pos: u16 = lastPos;

        var keyCount: u16 = 1;
        var kvSize = BTREE_PAGE_SIZE - HEADER - 10 * keyCount;

        while (find > 0) {
            kvSize = BTREE_PAGE_SIZE - HEADER - 10 * keyCount;
            pos = self.kvPos(find);

            //std.debug.print("KVSize{d} pos:{d} lastpos:{d}\n", .{ kvSize, pos, lastPos });
            if (lastPos - pos < kvSize) {
                keyCount = keyCount + 1;
                find = find - 1;
            } else {
                break;
            }
        }

        return find + 1;
    }

    // split a bigger-than-allowed node into two.
    // the second node always fits on a page.
    pub fn nodeSplit2(self: *BNode, right: *BNode, old: *BNode) void {
        const idx = old.findSplitIdx();
        std.debug.print("Split Index:{d} Old Node Type {d}", .{ idx, old.btype() });
        self.setHeader(old.btype(), idx);
        right.setHeader(old.btype(), old.nkeys() - idx);

        self.nodeAppendRange(old, 0, 0, idx);
        right.nodeAppendRange(old, 0, idx, old.nkeys() - idx);
    }

    // split a node if it's too big. the results are 1~3 nodes.
    pub fn nodeSplit3(self: *BNode, nodes: *SplitNodes) void {
        if (self.nbytes() <= BTREE_PAGE_SIZE) {
            nodes.Nodes[0].Copy(self);
            nodes.Count = 1;
            return;
        }

        var dataleft = [_]u8{0} ** (2 * BTREE_PAGE_SIZE);
        var dataright = [_]u8{0} ** BTREE_PAGE_SIZE;
        var left = BNode.initDoubleCapacityNode(&dataleft);
        var right = BNode.initSigleCapacity(&dataright);
        left.nodeSplit2(&right, self);

        if (left.nbytes() <= BTREE_PAGE_SIZE) {
            nodes.Nodes[0].Copy(&left);
            nodes.Nodes[1].Copy(&right);
            nodes.Count = 2;
            return;
        }

        // the left node is still too large
        var dataleftleft = [_]u8{0} ** BTREE_PAGE_SIZE;
        var datamidlle = [_]u8{0} ** BTREE_PAGE_SIZE;
        var leftleft = BNode.initSigleCapacity(&dataleftleft);
        var middle = BNode.initSigleCapacity(&datamidlle);
        leftleft.nodeSplit2(&middle, &left);
        std.debug.assert(leftleft.nbytes() <= BTREE_PAGE_SIZE);

        nodes.Nodes[0].Copy(&leftleft);
        nodes.Nodes[1].Copy(&middle);
        nodes.Nodes[2].Copy(&right);
        nodes.Count = 3;
    }

    pub fn Copy(self: *BNode, source: *BNode) void {
        const ptrSource = source.getdata();
        const ptrDestince = self.getdata();
        std.mem.copyForwards(u8, ptrDestince[0..BTREE_PAGE_SIZE], ptrSource[0..BTREE_PAGE_SIZE]);
    }

    // merge 2 nodes into 1
    pub fn nodeMerge(self: *BNode, left: *BNode, right: *BNode) void {
        self.setHeader(left.btype(), left.nkeys() + right.nkeys());
        self.nodeAppendRange(left, 0, 0, left.nkeys());
        self.nodeAppendRange(right, left.nkeys(), 0, right.nkeys());
    }

    pub fn nodeReplace2Kid(self: *BNode, oldNode: *BNode, idx: u16, ptrMergedNode: u64, key: []const u8) void {
        //std.debug.print("nodereplace2Kid. idx:{d} key:{s} nkeys:{d} ptr:", .{ idx, key, oldNode.nkeys() });

        self.setHeader(BNODE_NODE, oldNode.nkeys() - 1);
        //oldNode.print();

        self.nodeAppendRange(oldNode, 0, 0, idx);
        self.nodeAppendKV(idx, ptrMergedNode, key, "");
        self.nodeAppendRange(oldNode, idx + 1, idx + 2, oldNode.nkeys() - idx - 2);

        //std.debug.print("Node after nodereplace2kid.\n", .{});
        //newNode.print();
    }
};
