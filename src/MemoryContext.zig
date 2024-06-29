const std = @import("std");
const node = @import("BNode.zig");
const context = @import("KVContext.zig");

pub const MemoryContext = struct {
    root: u64,
    pages: std.AutoHashMap(u64, *node.BNode),
    allocator: std.mem.Allocator,

    pub fn init(self: *MemoryContext, allocator: std.mem.Allocator) !void {
        self.pages = std.AutoHashMap(u64, *node.BNode).init(allocator);
        self.root = 0;
        self.allocator = allocator;
    }

    pub fn deinit(self: *MemoryContext) void {
        var iterator = self.pages.iterator();
        while (iterator.next()) |entry| {
            const n = entry.value_ptr.*;
            const ptr = n.data.Single;
            self.allocator.destroy(n);
            self.allocator.free(ptr);
        }

        self.pages.deinit();
        self.allocator.destroy(self);
    }

    pub fn open(_: *MemoryContext) context.ContextError!void {}

    pub fn close(_: *MemoryContext) context.ContextError!void {}

    pub fn getRoot(self: *MemoryContext) u64 {
        return self.root;
    }

    pub fn setRoot(self: *MemoryContext, ptr: u64) void {
        self.root = ptr;
    }

    pub fn save(_: *MemoryContext) context.ContextError!void {
        //std.debug.print("Save Key List: Count:{d}\n", .{30});
    }

    fn printKeys(self: *MemoryContext) void {
        var iterator = self.pages.iterator();
        std.debug.print("Key List: Count:{d}\n", .{self.pages.count()});
        while (iterator.next()) |entry| {
            self.printNodePtr(entry.key_ptr.*);
        }
    }

    fn printNodePtr(_: MemoryContext, value: u64) void {
        var bytes: [8]u8 = undefined;
        const pos: u8 = 0;
        bytes[pos + 0] = @intCast((value >> 56) & 0xFF);
        bytes[pos + 1] = @intCast((value >> 48) & 0xFF);
        bytes[pos + 2] = @intCast((value >> 40) & 0xFF);
        bytes[pos + 3] = @intCast((value >> 32) & 0xFF);
        bytes[pos + 4] = @intCast((value >> 24) & 0xFF);
        bytes[pos + 5] = @intCast((value >> 16) & 0xFF);
        bytes[pos + 6] = @intCast((value >> 8) & 0xFF);
        bytes[pos + 7] = @intCast(value & 0xFF);

        std.debug.print("Key:", .{});
        for (bytes) |byte| {
            std.debug.print("{x} ", .{byte});
        }
        std.debug.print("\n", .{});
    }

    pub fn get(self: *MemoryContext, ptr: u64) context.ContextError!node.BNode {
        const value = self.pages.get(ptr);
        if (value) |v| {
            return v.*;
        } else {
            return context.ContextError.GetNodeError;
        }

        return context.ContextError.GetNodeError;
    }

    pub fn del(self: *MemoryContext, ptr: u64) bool {
        //.debug.print("del :{d} \n", .{ptr});

        const kv = self.pages.get(ptr);
        if (kv != null) {
            const n = kv.?;
            const ptr1 = n.data.Single;
            self.allocator.destroy(n);
            self.allocator.free(ptr1);

            return self.pages.remove(ptr);
        } else {
            return false;
        }
    }

    pub fn new(self: *MemoryContext, bnode: *node.BNode) context.ContextError!u64 {
        //std.debug.print("Pages Count:{d} \n", .{self.pages.count()});

        const ptr = bnode.getdata();
        var ptrData = self.allocator.alloc(u8, node.BTREE_PAGE_SIZE) catch {
            unreachable;
        };
        const newnode = self.allocator.create(node.BNode) catch {
            unreachable;
        };
        newnode.* = node.BNode{ .data = @unionInit(node.BNodeData, "Single", ptrData[0..node.BTREE_PAGE_SIZE]) };
        @memcpy(ptrData[0..node.BTREE_PAGE_SIZE], ptr[0..node.BTREE_PAGE_SIZE]);
        //newnode.print();

        const ptrToFirstElement = ptrData;
        const voidPtr: *const void = @ptrCast(ptrToFirstElement);
        const intRepresentation = @intFromPtr(voidPtr);
        const key: u64 = @intCast(intRepresentation);
        std.debug.assert(self.pages.contains(key) == false);
        self.pages.put(key, newnode) catch {
            return context.ContextError.DuplicateKey;
        };

        return key;
    }
};
