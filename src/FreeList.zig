const std = @import("std");
const node = @import("BNode.zig");
const context = @import("WinFreeListContext.zig");

pub const BNODE_FREE_LIST: u16 = 3;
pub const FREE_LIST_HEADER: u16 = 4 + 8 + 8;
pub const FREE_LIST_CAP: u16 = (node.BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8;

pub const FreeList = struct {
    head: u64,
    dbContext: *context.WindowsFreeListContext,
    allocator: std.mem.Allocator,

    pub fn init(db: *context.WindowsFreeListContext, allocator: std.mem.Allocator, head: u64) FreeList {
        return FreeList{ .head = head, .dbContext = db, .allocator = allocator };
    }

    pub fn Total(self: *FreeList) u64 {
        var node1 = self.dbContext.get(self.head) catch {
            unreachable;
        };
        return flnGetTotal(&node1);
    }

    pub fn Get(self: *FreeList, topN: u16) u64 {
        std.debug.assert(topN >= 0 and topN < self.Total());
        var count = topN;

        var curNode = self.dbContext.get(self.head) catch {
            unreachable;
        };
        while (flnSize(&curNode) <= count) {
            count -= flnSize(&curNode);
            const next = flnNext(&curNode);
            std.debug.assert(next != 0);
            curNode = self.dbContext.get(next) catch {
                unreachable;
            };
        }

        return flnPtr(&curNode, flnSize(&curNode) - count - 1);
    }

    // remove `popn` pointers and add some new pointers
    pub fn Update(self: *FreeList, popn: u16, freed: []const u64) void {
        std.debug.assert(popn <= self.Total());
        //std.debug.print("Total:{d} PopN:{d} FreeList Len :{d}\n", .{ self.Total(), popn, freed.len });

        if (popn == 0 and freed.len == 0)
            return;

        // prepare to construct the new list
        var total = self.Total();
        var count = popn;
        var listReuse = std.ArrayList(u64).init(self.allocator);
        var listFreeNode = std.ArrayList(u64).init(self.allocator);
        defer listReuse.deinit();
        defer listFreeNode.deinit();

        for (freed) |freenode| {
            listFreeNode.append(freenode) catch {
                unreachable;
            };
        }

        while (self.head != 0 and listReuse.items.len * FREE_LIST_CAP < listFreeNode.items.len) {
            var node1 = self.dbContext.get(self.head) catch {
                unreachable;
            };
            listFreeNode.append(self.head) catch {
                unreachable;
            };

            //std.debug.print("Head Ptr:{d}  Size {d}\n", .{ self.head, flnSize(node1) });

            if (count >= flnSize(&node1)) {
                // remove all pointers in this node
                count -= flnSize(&node1);
            } else {
                // remove some pointers
                var remain = flnSize(&node1) - count;
                count = 0;

                // reuse pointers from the free list itself
                while (remain > 0 and listReuse.items.len * FREE_LIST_CAP < listFreeNode.items.len + remain) {
                    //std.debug.print("Handle Remain.\n", .{});
                    remain -= 1;
                    listReuse.append(flnPtr(&node1, remain)) catch {
                        unreachable;
                    };
                }
                // move the node into the `freed` list
                for (0..remain) |idx| {
                    //std.debug.print("Handle Freed. {d}\n", .{idx});
                    const i: u16 = @intCast(idx);
                    listFreeNode.append(flnPtr(&node1, i)) catch {
                        unreachable;
                    };
                }
            }
            total -= flnSize(&node1);
            self.head = flnNext(&node1);
        }

        const newTotal = total + listFreeNode.items.len;
        std.debug.assert(listReuse.items.len * FREE_LIST_CAP >= listReuse.items.len or self.head == 0);
        self.flPush(&listFreeNode, &listReuse);

        var headnode = self.dbContext.get(self.head) catch {
            unreachable;
        };
        //std.debug.print("Set Total:{d}  Ptr:{d}\n", .{ newTotal, self.head });
        flnSetTotal(&headnode, newTotal);
    }

    pub fn flPush(self: *FreeList, listFreeNode: *std.ArrayList(u64), listReuse: *std.ArrayList(u64)) void {
        while (listFreeNode.items.len > 0) {
            var newNode = node.BNode.init(node.Type.Single);

            //construc new node
            var size: u16 = @intCast(listFreeNode.items.len);
            if (size > FREE_LIST_CAP)
                size = FREE_LIST_CAP;

            flnSetHeader(&newNode, size, self.head);

            for (0..size) |idx| {
                const i: u16 = @intCast(idx);
                const ptr = listFreeNode.pop();
                flnSetPtr(&newNode, i, ptr);
                //std.debug.print("Free node Ptr:{d}\n", .{ptr});
            }

            if (listReuse.items.len > 0) {
                //reuse a pointer from the list
                const ptrHead = listReuse.pop();
                self.head = ptrHead;
                //std.debug.print("Reuse Ptr {d} \n", .{self.head});
                self.dbContext.use(self.head, newNode);
            } else {
                self.head = self.dbContext.append(newNode);
                //std.debug.print("New Head Ptr {d} \n", .{self.head});
            }
        }

        std.debug.assert(listReuse.items.len == 0);
    }
};

pub fn flnSetHeader(self: *node.BNode, keynumber: u16, next: u64) void {
    const ptr = self.getdata();

    ptr[0] = @intCast(BNODE_FREE_LIST >> 8);
    ptr[1] = @intCast(BNODE_FREE_LIST & 0xFF);

    ptr[2] = @intCast(keynumber >> 8);
    ptr[3] = @intCast(keynumber & 0xFF);

    const pos = node.HEADER + 8;

    ptr[pos + 0] = @intCast((next >> 56) & 0xFF);
    ptr[pos + 1] = @intCast((next >> 48) & 0xFF);
    ptr[pos + 2] = @intCast((next >> 40) & 0xFF);
    ptr[pos + 3] = @intCast((next >> 32) & 0xFF);
    ptr[pos + 4] = @intCast((next >> 24) & 0xFF);
    ptr[pos + 5] = @intCast((next >> 16) & 0xFF);
    ptr[pos + 6] = @intCast((next >> 8) & 0xFF);
    ptr[pos + 7] = @intCast(next & 0xFF);
}

pub fn flnSize(self: *node.BNode) u16 {
    const ptr = self.getdata();
    const btype = self.btype();
    std.debug.assert(btype == BNODE_FREE_LIST);

    const high: u16 = @as(u16, ptr[2]) << 8;
    const low: u16 = @as(u16, ptr[3]);
    const result: u16 = high | low;

    return result;
}

pub fn flnNext(self: *node.BNode) u64 {
    const ptr = self.getdata();
    const btype = self.btype();
    std.debug.assert(btype == BNODE_FREE_LIST);

    const pos = node.HEADER + 8;

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

pub fn flnPtr(self: *node.BNode, idx: u16) u64 {
    const ptr = self.getdata();
    const btype = self.btype();
    std.debug.assert(btype == BNODE_FREE_LIST);

    const pos = FREE_LIST_HEADER + 8 * idx;

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

pub fn flnSetPtr(self: *node.BNode, idx: u16, value: u64) void {
    const ptr = self.getdata();
    std.debug.assert(idx < flnSize(self));
    const pos = FREE_LIST_HEADER + 8 * idx;

    ptr[pos + 0] = @intCast((value >> 56) & 0xFF);
    ptr[pos + 1] = @intCast((value >> 48) & 0xFF);
    ptr[pos + 2] = @intCast((value >> 40) & 0xFF);
    ptr[pos + 3] = @intCast((value >> 32) & 0xFF);
    ptr[pos + 4] = @intCast((value >> 24) & 0xFF);
    ptr[pos + 5] = @intCast((value >> 16) & 0xFF);
    ptr[pos + 6] = @intCast((value >> 8) & 0xFF);
    ptr[pos + 7] = @intCast(value & 0xFF);
}

pub fn flnSetTotal(self: *node.BNode, value: u64) void {
    const ptr = self.getdata();
    const pos = 4;

    ptr[pos + 0] = @intCast((value >> 56) & 0xFF);
    ptr[pos + 1] = @intCast((value >> 48) & 0xFF);
    ptr[pos + 2] = @intCast((value >> 40) & 0xFF);
    ptr[pos + 3] = @intCast((value >> 32) & 0xFF);
    ptr[pos + 4] = @intCast((value >> 24) & 0xFF);
    ptr[pos + 5] = @intCast((value >> 16) & 0xFF);
    ptr[pos + 6] = @intCast((value >> 8) & 0xFF);
    ptr[pos + 7] = @intCast(value & 0xFF);
}

pub fn flnGetTotal(self: *node.BNode) u64 {
    const ptr = self.getdata();
    const pos = 4;

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
