const std = @import("std");

pub fn compareArrays(a: []const u8, b: []const u8) i32 {
    const min_length = if (a.len < b.len) a.len else b.len;

    for (0..min_length) |i| {
        if (a[i] < b[i]) {
            return -1;
        } else if (a[i] > b[i]) {
            return 1;
        }
    }
    if (a.len < b.len) {
        return -1;
    } else if (a.len > b.len) {
        return 1;
    }
    return 0;
}

pub fn i8ToU8Array(value: i8) [1]u8 {
    var v = [_]u8{0} ** 1;
    v[0] = @intCast(value);

    return v;
}

pub fn i16ToU8Array(value: i16) [2]u8 {
    var data = [_]u8{0} ** 2;

    data[0] = @intCast((value >> 8) & 0xFF);
    data[1] = @intCast(value & 0xFF);

    return data;
}

pub fn i32ToU8Array(value: i32) [4]u8 {
    var data = [_]u8{0} ** 4;

    data[0] = @intCast((value >> 24) & 0xFF);
    data[1] = @intCast((value >> 16) & 0xFF);
    data[2] = @intCast((value >> 8) & 0xFF);
    data[3] = @intCast(value & 0xFF);

    return data;
}

pub fn i64ToU8Array(value: i64) [8]u8 {
    var data = [_]u8{0} ** 8;

    data[0] = @intCast((value >> 56) & 0xFF);
    data[1] = @intCast((value >> 48) & 0xFF);
    data[2] = @intCast((value >> 40) & 0xFF);
    data[3] = @intCast((value >> 32) & 0xFF);
    data[4] = @intCast((value >> 24) & 0xFF);
    data[5] = @intCast((value >> 16) & 0xFF);
    data[6] = @intCast((value >> 8) & 0xFF);
    data[7] = @intCast(value & 0xFF);

    return data;
}

pub fn U8ArrayToi64(data: []const u8) i64 {
    if (data.len != 8) {
        @panic("Slice length does not match expected length");
    }

    var number: i64 = 0;
    number |= @as(i64, data[0]) << 56;
    number |= @as(i64, data[1]) << 48;
    number |= @as(i64, data[2]) << 40;
    number |= @as(i64, data[3]) << 32;
    number |= @as(i64, data[4]) << 24;
    number |= @as(i64, data[5]) << 16;
    number |= @as(i64, data[6]) << 8;
    number |= @as(i64, data[7]);

    return number;
}

pub fn U8ArrayToi32(data: []const u8) i32 {
    if (data.len != 4) {
        @panic("Slice length does not match expected length");
    }

    var number: i32 = 0;
    number |= @as(i32, data[0]) << 24;
    number |= @as(i32, data[1]) << 16;
    number |= @as(i32, data[2]) << 8;
    number |= @as(i32, data[3]);

    return number;
}

pub fn U8ArrayToi16(data: []const u8) i16 {
    if (data.len != 2) {
        @panic("Slice length does not match expected length");
    }

    var number: i16 = 0;
    number |= @as(i16, data[0]) << 8;
    number |= @as(i16, data[1]);

    return number;
}

pub fn U8ArrayToi8(data: u8) i8 {
    const number: i8 = @intCast(data);
    return number;
}
