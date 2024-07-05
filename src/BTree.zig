const std = @import("std");
const node = @import("BNode.zig");
const context = @import("KVContext.zig");
const util = @import("Util.zig");
const biter = @import("BIter.zig");
const req = @import("Request.zig");

// modes of the updates
pub const MODE_UPSERT: u16 = 0; // insert or replac
pub const MODE_UPDATE_ONLY: u16 = 1; // update existing keys
pub const MODE_INSERT_ONLY: u16 = 2; // only add new keys

pub const BTreeError = error{
    RecordNotFoundError,
    PrimaryKeyExitError,
    SystemError,
};

pub const BTree = struct {
    kv: *context.KVContext,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, kv: *context.KVContext) !BTree {
        return BTree{ .kv = kv, .allocator = allocator };
    }

    pub fn init1(self: *BTree, allocator: std.mem.Allocator, kv: *context.KVContext) !void {
        self.allocator = allocator;
        self.kv = kv;
    }

    pub fn print(self: *BTree) void {
        std.debug.print("BTree content: Root:{d} \n", .{self.kv.getRoot()});

        if (self.kv.getRoot() == 0)
            return;

        var nodeRoot = self.kv.get(self.kv.getRoot()) catch {
            std.debug.panic("Get Node Exception!", .{});
        };
        self.printNode(&nodeRoot);
        std.debug.print("\n", .{});
    }

    pub fn printNode(self: *BTree, treenode: *node.BNode) void {
        if (treenode.btype() == node.BNODE_LEAF) {
            treenode.print();
        } else if (treenode.btype() == node.BNODE_FREE_LIST) {
            //treenode.print();
        } else {
            treenode.print();
            const nkeys = treenode.nkeys();
            std.debug.print("NKeys {d}", .{nkeys});
            var idx: u16 = 0;
            while (idx < nkeys) {
                const prtNode = treenode.getPtr(idx);
                var subNode = self.kv.get(prtNode) catch {
                    std.debug.panic("Get Node Exception!", .{});
                };
                self.printNode(&subNode);

                idx = idx + 1;
            }
        }
    }

    pub fn deinit(_: *BTree) void {
        //self.kv.close();
        //self.kv.deinit();
    }

    pub fn Get(self: *BTree, key: []const u8) ?[]u8 {
        var rootNode = self.kv.get(self.kv.getRoot()) catch unreachable;
        return self.treeSearch(&rootNode, key);
    }

    pub fn Set(self: *BTree, key: []const u8, val: []const u8, mode: u16) !void {
        try self.InsertKV(key, val, mode);
        try self.kv.save();
    }

    pub fn SetEx(self: *BTree, request: *req.InsertReqest) !void {
        try self.InsertKVEx(request);
        try self.kv.save();
    }

    pub fn Delete(self: *BTree, key: []const u8) !bool {
        const ret = try self.DeleteKV(key);
        if (ret == true) {
            try self.kv.save();
            return true;
        } else {
            return false;
        }
    }

    pub fn DeleteEx(self: *BTree, request: *req.DeleteRequest) !bool {
        const ret = try self.DeleteKVEx(request);
        if (ret == true) {
            try self.kv.save();
            return true;
        } else {
            return false;
        }
    }

    // find the closest position that is less or equal to the input key
    pub fn SeekLE(self: *BTree, key: []const u8) !*biter.BIter {
        var iter = try self.allocator.create(biter.BIter);
        iter.init1(self.allocator, self.kv);

        var ptr = self.getRoot();
        var n = try self.getNode(ptr);
        var idx: u16 = 0;
        while (ptr != 0) {
            n = try self.getNode(ptr);
            idx = n.nodeLookupLE(key);
            try iter.path.append(n);
            try iter.pos.append(idx);
            if (n.btype() == node.BNODE_NODE) {
                ptr = n.getPtr(idx);
            } else {
                ptr = 0;
            }
        }
        iter.valid = true;
        return iter;
    }

    // find the closest position to a key with respect to the `cmp` relation
    pub fn Seek(self: *BTree, key: []const u8, cmp: biter.OP_CMP) !*biter.BIter {
        var iter = try self.SeekLE(key);
        if (cmp != biter.OP_CMP.CMP_LE and iter.Valid()) {
            const cur = iter.Deref();

            if (biter.cmpOK(cur.key, key, cmp) == false) {
                //off by one
                if (@intFromEnum(cmp) > 0) {
                    _ = iter.Next();
                } else {
                    _ = iter.Prev();
                }
            }
        }
        return iter;
    }

    //Interface for Insert KV
    pub fn InsertKV(self: *BTree, key: []const u8, val: []const u8, mode: u16) !void {
        std.debug.assert(key.len != 0);
        std.debug.assert(key.len <= node.BTREE_MAX_KEY_SIZE);
        std.debug.assert(val.len <= node.BTREE_MAX_VALUE_SIZE);

        if (self.kv.getRoot() == 0) {
            var data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
            var root = node.BNode.initSigleCapacity(&data);
            root.setHeader(node.BNODE_LEAF, 2);
            root.nodeAppendKV(0, 0, "", "");
            root.nodeAppendKV(1, 0, key, val);
            const newroot = try self.kv.new(&root);
            self.kv.setRoot(newroot);
            return;
        }

        const oldRootPtr = self.kv.getRoot();
        var nodeRoot = try self.kv.get(oldRootPtr);

        var data2 = [_]u8{0} ** (2 * node.BTREE_PAGE_SIZE);
        var nodeTmp = node.BNode.initDoubleCapacityNode(&data2);

        try self.treeInsert(&nodeRoot, key, val, mode, &nodeTmp);

        var n1data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
        var n2data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
        var n3data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
        var n1 = node.BNode.initSigleCapacity(&n1data);
        var n2 = node.BNode.initSigleCapacity(&n2data);
        var n3 = node.BNode.initSigleCapacity(&n3data);

        var subNodes = node.SplitNodes{ .Count = 0, .Nodes = .{ &n1, &n2, &n3 } };
        nodeTmp.nodeSplit3(&subNodes);
        if (subNodes.Count > 1) {
            // the root was split, add a new level.
            var data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
            var newRoot = node.BNode.initSigleCapacity(&data);
            newRoot.setHeader(node.BNODE_NODE, subNodes.Count);
            var i: u16 = 0;
            while (i < subNodes.Count) {
                const ptr = self.kv.new(subNodes.Nodes[i]) catch {
                    std.debug.panic("Create New Node failed\n", .{});
                };
                const nodekey = subNodes.Nodes[i].getKey(0);
                newRoot.nodeAppendKV(i, ptr, nodekey, "");
                i += 1;
            }
            const rootPtr = self.kv.new(&newRoot) catch {
                std.debug.panic("Create New Node failed\n", .{});
            };
            self.kv.setRoot(rootPtr);

            //self.print();
        } else {
            const ptr1 = self.kv.new(subNodes.Nodes[0]) catch {
                std.debug.panic("Create New Node failed\n", .{});
            };
            self.kv.setRoot(ptr1);
        }
        _ = self.kv.del(oldRootPtr);
    }

    //Interface for Insert KV
    pub fn InsertKVEx(self: *BTree, request: *req.InsertReqest) !void {
        std.debug.assert(request.Key.len != 0);
        std.debug.assert(request.Key.len <= node.BTREE_MAX_KEY_SIZE);
        std.debug.assert(request.Key.len <= node.BTREE_MAX_VALUE_SIZE);

        if (self.kv.getRoot() == 0) {
            var data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
            var root = node.BNode.initSigleCapacity(&data);
            root.setHeader(node.BNODE_LEAF, 2);
            root.nodeAppendKV(0, 0, "", "");
            root.nodeAppendKV(1, 0, request.Key, request.Val);
            const newroot = try self.kv.new(&root);
            self.kv.setRoot(newroot);
            return;
        }

        const oldRootPtr = self.kv.getRoot();
        var nodeRoot = try self.kv.get(oldRootPtr);

        var data2 = [_]u8{0} ** (2 * node.BTREE_PAGE_SIZE);
        var nodeTmp = node.BNode.initDoubleCapacityNode(&data2);

        try self.treeInsertEx(&nodeRoot, request, &nodeTmp);

        var n1data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
        var n2data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
        var n3data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
        var n1 = node.BNode.initSigleCapacity(&n1data);
        var n2 = node.BNode.initSigleCapacity(&n2data);
        var n3 = node.BNode.initSigleCapacity(&n3data);

        var subNodes = node.SplitNodes{ .Count = 0, .Nodes = .{ &n1, &n2, &n3 } };
        nodeTmp.nodeSplit3(&subNodes);
        if (subNodes.Count > 1) {
            // the root was split, add a new level.
            var data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
            var newRoot = node.BNode.initSigleCapacity(&data);
            newRoot.setHeader(node.BNODE_NODE, subNodes.Count);
            var i: u16 = 0;
            while (i < subNodes.Count) {
                const ptr = self.kv.new(subNodes.Nodes[i]) catch {
                    std.debug.panic("Create New Node failed\n", .{});
                };
                const nodekey = subNodes.Nodes[i].getKey(0);
                newRoot.nodeAppendKV(i, ptr, nodekey, "");
                i += 1;
            }
            const rootPtr = self.kv.new(&newRoot) catch {
                std.debug.panic("Create New Node failed\n", .{});
            };
            self.kv.setRoot(rootPtr);

            //self.print();
        } else {
            const ptr1 = self.kv.new(subNodes.Nodes[0]) catch {
                std.debug.panic("Create New Node failed\n", .{});
            };
            self.kv.setRoot(ptr1);
        }
        _ = self.kv.del(oldRootPtr);
    }

    //Interface for Delete KV
    pub fn DeleteKV(self: *BTree, key: []const u8) !bool {
        std.debug.assert(key.len != 0);
        std.debug.assert(key.len <= node.BTREE_MAX_KEY_SIZE);

        if (self.kv.getRoot() == 0) {
            return false;
        }

        //const n1 = try self.kv.get(self.kv.getRoot());
        var n1 = self.kv.get(self.kv.getRoot()) catch {
            std.debug.panic("Root not found!", .{});
        };

        var data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
        var updatedNode = node.BNode.initSigleCapacity(&data);
        const ret = self.treeDelete(&n1, key, &updatedNode);
        if (ret) {
            _ = self.kv.del(self.kv.getRoot());
            if (updatedNode.btype() == node.BNODE_NODE and updatedNode.nkeys() == 1) {
                // remove a level
                self.kv.setRoot(updatedNode.getPtr(0));
            } else {
                const newroot = self.kv.new(&updatedNode) catch {
                    std.debug.panic("Create Node Exception!", .{});
                };
                self.kv.setRoot(newroot);
            }

            return true;
        } else return false;
    }

    //Interface for Delete KV
    pub fn DeleteKVEx(self: *BTree, request: *req.DeleteRequest) !bool {
        std.debug.assert(request.Key.len != 0);
        std.debug.assert(request.Key.len <= node.BTREE_MAX_KEY_SIZE);

        if (self.kv.getRoot() == 0) {
            return false;
        }

        //const n1 = try self.kv.get(self.kv.getRoot());
        var n1 = self.kv.get(self.kv.getRoot()) catch {
            std.debug.panic("Root not found!", .{});
        };

        var data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
        var updatedNode = node.BNode.initSigleCapacity(&data);
        const ret = self.treeDeleteEx(&n1, request, &updatedNode);
        if (ret) {
            _ = self.kv.del(self.kv.getRoot());
            if (updatedNode.btype() == node.BNODE_NODE and updatedNode.nkeys() == 1) {
                // remove a level
                self.kv.setRoot(updatedNode.getPtr(0));
            } else {
                const newroot = self.kv.new(&updatedNode) catch {
                    std.debug.panic("Create Node Exception!", .{});
                };
                self.kv.setRoot(newroot);
            }

            return true;
        } else return false;
    }

    pub fn getNode(self: *BTree, ptr: u64) !node.BNode {
        return self.kv.get(ptr);
    }

    pub fn getRoot(self: *BTree) u64 {
        return self.kv.getRoot();
    }
    // Search a key from the tree
    pub fn treeSearch(self: *BTree, treenode: *node.BNode, key: []const u8) ?[]u8 {
        // where to find the key?
        const idx = treenode.nodeLookupLE(key);
        // act depending on the node type
        switch (treenode.btype()) {
            node.BNODE_LEAF => {
                // leaf, node.getKey(idx) <= key
                const comp = util.compareArrays(key, treenode.getKey(idx));
                if (comp == 0) {
                    return treenode.getValue(idx);
                } else {
                    // not found
                    return null;
                }
            },
            node.BNODE_NODE => {
                const ptr = treenode.getPtr(idx);
                var subNode = self.kv.get(ptr) catch unreachable;

                // internal node, insert it to a kid node.
                return self.treeSearch(&subNode, key);
            },
            else => {
                std.debug.panic("Exception Insert Node!\n", .{});
            },
        }
    }

    // delete a key from the tree
    pub fn treeDelete(self: *BTree, treenode: *node.BNode, key: []const u8, updatedNode: *node.BNode) bool {
        // where to find the key?
        const idx = treenode.nodeLookupLE(key);
        // act depending on the node type
        switch (treenode.btype()) {
            node.BNODE_LEAF => {
                // leaf, node.getKey(idx) <= key
                const comp = util.compareArrays(key, treenode.getKey(idx));
                if (comp == 0) {
                    // delete the key in the leaf
                    //std.debug.print("Node Delete! {d}", .{idx});
                    //treenode.print();
                    updatedNode.leafDelete(treenode, idx);
                    //updatedNode.print();
                    return true;
                } else {
                    // not found
                    return false;
                }
            },
            node.BNODE_NODE => {
                // internal node, insert it to a kid node.
                return self.nodeDelete(treenode, idx, key, updatedNode);
            },
            else => {
                std.debug.panic("Exception Insert Node!\n", .{});
            },
        }
    }

    // delete a key from the tree
    pub fn treeDeleteEx(self: *BTree, treenode: *node.BNode, request: *req.DeleteRequest, updatedNode: *node.BNode) bool {
        // where to find the key?
        const idx = treenode.nodeLookupLE(request.Key);
        // act depending on the node type
        switch (treenode.btype()) {
            node.BNODE_LEAF => {
                // leaf, node.getKey(idx) <= key
                const comp = util.compareArrays(request.Key, treenode.getKey(idx));
                if (comp == 0) {
                    // delete the key in the leaf
                    //std.debug.print("Node Delete! {d}", .{idx});
                    //treenode.print();
                    updatedNode.leafDelete(treenode, idx);
                    const v = treenode.getValue(idx);
                    request.OldValue.appendSlice(v) catch {
                        return false;
                    };

                    //updatedNode.print();
                    return true;
                } else {
                    // not found
                    return false;
                }
            },
            node.BNODE_NODE => {
                // internal node, insert it to a kid node.
                return self.nodeDeleteEx(treenode, idx, request, updatedNode);
            },
            else => {
                std.debug.panic("Exception Insert Node!\n", .{});
            },
        }
    }

    pub fn nodeDelete(self: *BTree, treenode: *node.BNode, idx: u16, key: []const u8, updatedNode: *node.BNode) bool {
        // recurse into the kid
        const kptr = treenode.getPtr(idx);
        var realnode = self.kv.get(kptr) catch {
            std.debug.panic("Node is not found! idx:{d} Key:{s}", .{ idx, key });
        };

        var data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
        var nodeTmp = node.BNode.initSigleCapacity(&data);
        const updated = self.treeDelete(&realnode, key, &nodeTmp);
        if (updated == false) {
            return false; // not found
        }
        //nodeTmp.print();
        _ = self.kv.del(kptr);

        // check for merging
        const ret = self.shouldMerge(treenode, idx, &nodeTmp);
        switch (ret.MergeFlag) {
            0 => {
                std.debug.assert(nodeTmp.nkeys() > 0);
                nodeReplaceKidN(self, updatedNode, treenode, idx, &node.SplitNodes{ .Count = 1, .Nodes = [3]*node.BNode{ &nodeTmp, undefined, undefined } });
            },
            -1 => { //left
                //std.debug.print("Merge Left.\n", .{});

                var dataMergedLeft = [_]u8{0} ** node.BTREE_PAGE_SIZE;
                var mergedleft = node.BNode.initSigleCapacity(&dataMergedLeft);

                var nodeMerged = self.kv.get(ret.ptrNode) catch {
                    std.debug.panic("Get Node Exception idx: {d}", .{ret.ptrNode});
                };
                mergedleft.nodeMerge(&nodeMerged, &nodeTmp);
                const prtMergedNodeleft = self.kv.new(&mergedleft) catch {
                    std.debug.panic("Create New Node Exception", .{});
                };

                _ = self.kv.del(treenode.getPtr(idx - 1));
                updatedNode.nodeReplace2Kid(treenode, idx - 1, prtMergedNodeleft, mergedleft.getKey(0));
            },
            1 => { //right
                //std.debug.print("Merge Right.\n", .{});

                var nodeMerged = self.kv.get(ret.ptrNode) catch {
                    std.debug.panic("Get Node Exception idx: {d}", .{ret.ptrNode});
                };

                var datamergedright = [_]u8{0} ** node.BTREE_PAGE_SIZE;
                var mergedright = node.BNode.initSigleCapacity(&datamergedright);
                mergedright.nodeMerge(&nodeTmp, &nodeMerged);
                _ = self.kv.del(treenode.getPtr(idx + 1));
                const prtMergedNodeRight = self.kv.new(&mergedright) catch {
                    std.debug.panic("Create New Node Exception", .{});
                };
                updatedNode.nodeReplace2Kid(treenode, idx, prtMergedNodeRight, mergedright.getKey(0));
            },
            else => {
                std.debug.panic("Exception Merge Flag!\n", .{});
            },
        }

        return true;
    }

    pub fn nodeDeleteEx(self: *BTree, treenode: *node.BNode, idx: u16, request: *req.DeleteRequest, updatedNode: *node.BNode) bool {
        // recurse into the kid
        const kptr = treenode.getPtr(idx);
        var realnode = self.kv.get(kptr) catch {
            std.debug.panic("Node is not found! idx:{d} Key:{s}", .{ idx, request.Key });
        };

        var data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
        var nodeTmp = node.BNode.initSigleCapacity(&data);
        const updated = self.treeDeleteEx(&realnode, request, &nodeTmp);
        if (updated == false) {
            return false; // not found
        }
        //nodeTmp.print();
        _ = self.kv.del(kptr);

        // check for merging
        const ret = self.shouldMerge(treenode, idx, &nodeTmp);
        switch (ret.MergeFlag) {
            0 => {
                std.debug.assert(nodeTmp.nkeys() > 0);
                nodeReplaceKidN(self, updatedNode, treenode, idx, &node.SplitNodes{ .Count = 1, .Nodes = [3]*node.BNode{ &nodeTmp, undefined, undefined } });
            },
            -1 => { //left
                //std.debug.print("Merge Left.\n", .{});

                var dataMergedLeft = [_]u8{0} ** node.BTREE_PAGE_SIZE;
                var mergedleft = node.BNode.initSigleCapacity(&dataMergedLeft);

                var nodeMerged = self.kv.get(ret.ptrNode) catch {
                    std.debug.panic("Get Node Exception idx: {d}", .{ret.ptrNode});
                };
                mergedleft.nodeMerge(&nodeMerged, &nodeTmp);
                const prtMergedNodeleft = self.kv.new(&mergedleft) catch {
                    std.debug.panic("Create New Node Exception", .{});
                };

                _ = self.kv.del(treenode.getPtr(idx - 1));
                updatedNode.nodeReplace2Kid(treenode, idx - 1, prtMergedNodeleft, mergedleft.getKey(0));
            },
            1 => { //right
                //std.debug.print("Merge Right.\n", .{});

                var nodeMerged = self.kv.get(ret.ptrNode) catch {
                    std.debug.panic("Get Node Exception idx: {d}", .{ret.ptrNode});
                };

                var datamergedright = [_]u8{0} ** node.BTREE_PAGE_SIZE;
                var mergedright = node.BNode.initSigleCapacity(&datamergedright);
                mergedright.nodeMerge(&nodeTmp, &nodeMerged);
                _ = self.kv.del(treenode.getPtr(idx + 1));
                const prtMergedNodeRight = self.kv.new(&mergedright) catch {
                    std.debug.panic("Create New Node Exception", .{});
                };
                updatedNode.nodeReplace2Kid(treenode, idx, prtMergedNodeRight, mergedright.getKey(0));
            },
            else => {
                std.debug.panic("Exception Merge Flag!\n", .{});
            },
        }

        return true;
    }
    //replace a link with mutile links
    pub fn nodeReplaceKidN(self: *BTree, newNode: *node.BNode, oldNode: *node.BNode, idx: u16, kids: *const node.SplitNodes) void {
        newNode.setHeader(node.BNODE_NODE, oldNode.nkeys() + kids.Count - 1);
        newNode.nodeAppendRange(oldNode, 0, 0, idx);

        var i: u16 = 0;
        while (i < kids.Count) {
            const n = self.kv.new(kids.Nodes[i]) catch {
                std.debug.panic("Create New Node failed\n", .{});
            };
            newNode.nodeAppendKV(idx + i, n, kids.Nodes[i].getKey(0), "");
            i += 1;
        }
        newNode.nodeAppendRange(oldNode, idx + kids.Count, idx + 1, oldNode.nkeys() - (idx + 1));
    }

    pub const ShouldMergeResult = struct { MergeFlag: i8, ptrNode: u64 };

    // should the updated kid be merged with a sibling?
    pub fn shouldMerge(self: *BTree, treenode: *node.BNode, idx: u16, updated: *node.BNode) ShouldMergeResult {
        if (updated.nbytes() > node.BTREE_PAGE_SIZE / 4) {
            return ShouldMergeResult{ .MergeFlag = 0, .ptrNode = 0 };
        }

        if (idx > 0) {
            var sibling = self.kv.get(treenode.getPtr(idx - 1)) catch {
                std.debug.panic("Get Node Exception idx: {d}", .{idx - 1});
            };
            const merged = sibling.nbytes() + updated.nbytes() - node.HEADER;
            if (merged <= node.BTREE_PAGE_SIZE) {
                return ShouldMergeResult{ .MergeFlag = -1, .ptrNode = treenode.getPtr(idx - 1) };
            }
        }
        if (idx + 1 < treenode.nkeys()) {
            var sibling1 = self.kv.get(treenode.getPtr(idx + 1)) catch {
                std.debug.panic("Get Node Exception idx: {d}", .{idx - 1});
            };
            const merged1 = sibling1.nbytes() + updated.nbytes() - node.HEADER;
            if (merged1 <= node.BTREE_PAGE_SIZE) {
                return ShouldMergeResult{ .MergeFlag = 1, .ptrNode = treenode.getPtr(idx + 1) };
            }
        }

        return ShouldMergeResult{ .MergeFlag = 0, .ptrNode = 0 };
    }

    // insert a KV into a node, the result might be split into 2 nodes.
    // the caller is responsible for deallocating the input node
    // and splitting and allocating result nodes.
    pub fn treeInsert(self: *BTree, oldNode: *node.BNode, key: []const u8, val: []const u8, mode: u16, newNode: *node.BNode) BTreeError!void {
        // where to insert the key?

        const idx = oldNode.nodeLookupLE(key);
        //std.debug.print("Find  Key:{s} Index:{d}", .{ key, idx });
        // act depending on the node type
        switch (oldNode.btype()) {
            node.BNODE_LEAF => {
                // leaf, node.getKey(idx) <= key
                const comp = util.compareArrays(key, oldNode.getKey(idx));
                if (comp == 0) {
                    if (mode == MODE_INSERT_ONLY) {
                        return BTreeError.PrimaryKeyExitError;
                    }
                    // found the key, update it.
                    newNode.leafUpdate(oldNode, idx, key, val);
                } else {
                    if (mode == MODE_UPDATE_ONLY) {
                        return BTreeError.RecordNotFoundError;
                    }
                    // insert it after the position.
                    newNode.leafInsert(oldNode, idx + 1, key, val);
                }
            },
            node.BNODE_NODE => {
                // internal node, insert it to a kid node.
                try self.nodeInsert(newNode, oldNode, idx, key, val, mode);
            },
            else => {
                std.debug.panic("Exception Insert Node!\n", .{});
                return BTreeError.Panic;
            },
        }
    }

    // insert a KV into a node, the result might be split into 2 nodes.
    // the caller is responsible for deallocating the input node
    // and splitting and allocating result nodes.
    pub fn treeInsertEx(self: *BTree, oldNode: *node.BNode, request: *req.InsertReqest, newNode: *node.BNode) BTreeError!void {
        // where to insert the key?

        const idx = oldNode.nodeLookupLE(request.Key);
        //std.debug.print("Find  Key:{s} Index:{d}", .{ key, idx });
        // act depending on the node type
        switch (oldNode.btype()) {
            node.BNODE_LEAF => {
                // leaf, node.getKey(idx) <= key
                const comp = util.compareArrays(request.Key, oldNode.getKey(idx));
                if (comp == 0) {
                    if (request.Mode == MODE_INSERT_ONLY) {
                        return BTreeError.PrimaryKeyExitError;
                    }
                    // found the key, update it.
                    newNode.leafUpdate(oldNode, idx, request.Key, request.Val);
                    const v = oldNode.getValue(idx);
                    request.OldValue.appendSlice(v) catch {
                        return BTreeError.SystemError;
                    };
                    request.Added = false;
                    request.Updated = true;
                } else {
                    if (request.Mode == MODE_UPDATE_ONLY) {
                        return BTreeError.RecordNotFoundError;
                    }
                    // insert it after the position.
                    newNode.leafInsert(oldNode, idx + 1, request.Key, request.Val);
                    request.Added = true;
                    request.Updated = true;
                }
            },
            node.BNODE_NODE => {
                // internal node, insert it to a kid node.
                try self.nodeInsertEx(newNode, oldNode, idx, request);
            },
            else => {
                std.debug.panic("Exception Insert Node!\n", .{});
                return BTreeError.Panic;
            },
        }
    }

    // part of the treeInsert(): KV insertion to an internal node
    pub fn nodeInsertEx(
        self: *BTree,
        newNode: *node.BNode,
        oldNode: *node.BNode,
        idx: u16,
        request: *req.InsertReqest,
    ) BTreeError!void {
        //get and deallocate the kid node
        const kptr = oldNode.getPtr(idx);
        var knode = self.kv.get(kptr) catch {
            std.debug.panic("Node is not found. Ptr:{d}\n", .{kptr});
        };

        // recursive insertion to the kid node
        var data2 = [_]u8{0} ** (2 * node.BTREE_PAGE_SIZE);
        var nodeTmp = node.BNode.initDoubleCapacityNode(&data2);

        try self.treeInsertEx(&knode, request, &nodeTmp);

        //split result
        var n1data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
        var n2data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
        var n3data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
        var n1 = node.BNode.initSigleCapacity(&n1data);
        var n2 = node.BNode.initSigleCapacity(&n2data);
        var n3 = node.BNode.initSigleCapacity(&n3data);

        var subNodes = node.SplitNodes{ .Count = 0, .Nodes = .{ &n1, &n2, &n3 } };
        nodeTmp.nodeSplit3(&subNodes);
        //std.debug.print("Split Count:{d}", .{subNodes.Count});
        //update the kid links
        self.nodeReplaceKidN(newNode, oldNode, idx, &subNodes);
        _ = self.kv.del(kptr);
    }

    // part of the treeInsert(): KV insertion to an internal node
    pub fn nodeInsert(self: *BTree, newNode: *node.BNode, oldNode: *node.BNode, idx: u16, key: []const u8, val: []const u8, mode: u16) BTreeError!void {
        //get and deallocate the kid node
        const kptr = oldNode.getPtr(idx);
        var knode = self.kv.get(kptr) catch {
            std.debug.panic("Node is not found. Ptr:{d}\n", .{kptr});
        };

        // recursive insertion to the kid node
        var data2 = [_]u8{0} ** (2 * node.BTREE_PAGE_SIZE);
        var nodeTmp = node.BNode.initDoubleCapacityNode(&data2);

        try self.treeInsert(&knode, key, val, mode, &nodeTmp);

        //split result
        var n1data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
        var n2data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
        var n3data = [_]u8{0} ** node.BTREE_PAGE_SIZE;
        var n1 = node.BNode.initSigleCapacity(&n1data);
        var n2 = node.BNode.initSigleCapacity(&n2data);
        var n3 = node.BNode.initSigleCapacity(&n3data);

        var subNodes = node.SplitNodes{ .Count = 0, .Nodes = .{ &n1, &n2, &n3 } };
        nodeTmp.nodeSplit3(&subNodes);
        //std.debug.print("Split Count:{d}", .{subNodes.Count});
        //update the kid links
        self.nodeReplaceKidN(newNode, oldNode, idx, &subNodes);
        _ = self.kv.del(kptr);
    }

    fn printNodeWithKey(treenode: *node.BNode, value: u64) void {
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
        treenode.print();
    }

    fn printNodePtr(value: u64) void {
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
};
