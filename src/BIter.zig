const std = @import("std");
const node = @import("BNode.zig");
const kvcontext = @import("KVContext.zig");
const util = @import("Util.zig");

pub const KVStruct = struct {
    key: []const u8,
    val: []const u8,
};

pub const BIterError = error{ PrevNotFound, NextNotFound };
pub const BIter = struct {
    kv: *kvcontext.KVContext,
    path: std.ArrayList(node.BNode), // from root to leaf
    pos: std.ArrayList(u16), //indexes into nodes
    allocator: std.mem.Allocator,
    valid: bool,

    pub fn init(allocator: std.mem.Allocator, kv: *kvcontext.KVContext) BIter {
        return BIter{ .allocator = allocator, .path = std.ArrayList(node.BNode).init(allocator), .pos = std.ArrayList(u16).init(allocator), .kv = kv, .valid = true };
    }

    pub fn init1(self: *BIter, allocator: std.mem.Allocator, kv: *kvcontext.KVContext) void {
        self.kv = kv;
        self.pos = std.ArrayList(u16).init(allocator);
        self.path = std.ArrayList(node.BNode).init(allocator);
        self.allocator = allocator;
    }

    pub fn deinit(self: *BIter) void {
        self.pos.deinit();
        self.path.deinit();

        self.allocator.destroy(self);
    }

    pub fn Deref(self: *BIter) KVStruct {
        //std.debug.print("Path Count:{d} Pos:{d} \n", .{ self.path.items.len, self.pos.getLast() });
        var n = self.path.getLast();
        return KVStruct{ .key = n.getKey(self.pos.getLast()), .val = n.getValue(self.pos.getLast()) };
    }

    pub fn Prev(self: *BIter) bool {
        const level: u16 = @intCast(self.path.items.len);
        self.interPrev(level - 1) catch {
            return false;
        };
        return true;
    }

    pub fn Next(self: *BIter) bool {
        const level: u16 = @intCast(self.path.items.len);
        self.interNext(level - 1) catch {
            return false;
        };
        return true;
    }

    pub fn Valid(self: *BIter) bool {
        return self.valid;
    }

    //           root
    //           1                        2 3
    //11      12             13
    //   121 122 123    131 132 133
    //133-》Prev =》 level 2 + Path(root,1,13) Pos(0,2,2) -> 132 Path(root,1,13,) Pos(0,2,1)
    //132-》Prev =》 level 2 + Path(root,1,13) Pos(0,2,1) -> 131 Path(root,1,13)  Pos(0,2,0)
    //131-》Prev =》 level 2 + Path(root,1,13) Pos(0,2,0) -> level 1 + Path(root,1) Pos(0,2) -> level 1 + Path(root,1) Pos(0,1) -> level 2 + Path(root,1,12) Pos(0,1,2)

    //122-》Next => level 2 + Path(root,1,12) Pos(0,1,1) -> 123  Path(root,1,12) Pos(0,1,2)
    //123-》Next => level 2 + Path(root,1,12) Pos(0,1,2) -> level 1 + Path(root,1) Pos(0,1) -> level 1 + Path(root,1) Pos(0,2) -> 131 level 2 + Path(root,1,13) Pos(0,1,0)
    pub fn interPrev(self: *BIter, level: u16) !void {
        if (self.pos.items[level] > 0) { // move within this node
            self.pos.items[level] -= 1;
        } else if (level > 0) { // move to a slibing node
            _ = self.path.pop();
            _ = self.pos.pop();
            try self.interPrev(level - 1);
        } else {
            self.valid = false;
            return BIterError.PrevNotFound;
        }

        if (level + 1 < self.pos.items.len) {
            // update the kid node

            var n = self.path.items[level];
            const idx = n.getPtr(self.pos.items[level]);
            var kid = try self.kv.get(idx);

            try self.path.append(kid);
            try self.pos.append(kid.nkeys() - 1);
        }
    }

    pub fn interNext(self: *BIter, level: u16) !void {
        if (self.pos.items[level] < self.path.items[level].nkeys() - 1) { // move within this node
            self.pos.items[level] += 1;
        } else if (level > 0) { // move to a slibing node
            _ = self.path.pop();
            _ = self.pos.pop();
            try self.interNext(level - 1);
        } else {
            self.valid = false;
            return BIterError.NextNotFound;
        }

        if (level + 1 < self.pos.items.len) {
            // update the kid node
            var n = self.path.items[level];
            const idx = n.getPtr(self.pos.items[level]);
            const kid = try self.kv.get(idx);

            try self.path.append(kid);
            try self.pos.append(0);
        }
    }
};

pub const OP_CMP = enum(i16) {
    CMP_GE = 3,
    CMP_GT = 2,
    CMP_LT = -2,
    CMP_LE = -3,
};

pub fn cmpOK(key: []const u8, ref: []const u8, cmp: OP_CMP) bool {
    const ret = util.compareArrays(key, ref);
    switch (cmp) {
        OP_CMP.CMP_GE => {
            return ret >= 0;
        },
        OP_CMP.CMP_GT => {
            return ret > 0;
        },
        OP_CMP.CMP_LT => {
            return ret < 0;
        },
        OP_CMP.CMP_LE => {
            return ret <= 0;
        },
    }
}
