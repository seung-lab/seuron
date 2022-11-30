def read_chunks(f, s_i, t_j, p_ji):
    import struct
    unpack_count = struct.Struct('q').unpack
    unpack_seg = struct.Struct('qq').unpack
    unpack_seg_pair = struct.Struct('qqq').unpack
    while True:
        count_data = f.read(8)
        if count_data:
            count, = unpack_count(count_data)
        else:
            return

        for i in range(count):
            seg, vx = unpack_seg(f.read(16))
            s_i[int(seg)] += vx

        count, = unpack_count(f.read(8))
        for i in range(count):
            seg, vx = unpack_seg(f.read(16))
            t_j[int(seg)] += vx

        count, = unpack_count(f.read(8))
        for i in range(count):
            s1, s2, vx = unpack_seg_pair(f.read(24))
            p_ji[int(s1)][int(s2)] += vx


def evaluate_rand(s_i, t_j, p_ji):
    sum_p_ji_square = sum(v * v for j in p_ji for v in p_ji[j].values())
    sum_s_i_square = sum(v * v for v in s_i.values())
    sum_t_j_square = sum(v * v for v in t_j.values())
    rand_split = sum_p_ji_square/sum_t_j_square
    rand_merge = sum_p_ji_square/sum_s_i_square

    return rand_split, rand_merge


def evaluate_voi(s_i, t_j, p_ji):
    from math import log

    total = sum(s_i.values())
    H_st = - sum(v/total * log(v/total) for j in p_ji for v in p_ji[j].values())
    H_s = - sum(v/total * log(v/total) for v in s_i.values())
    H_t = - sum(v/total * log(v/total) for v in t_j.values())
    voi_split = H_st - H_t
    voi_merge = H_st - H_s

    return voi_split, voi_merge

def find_large_diff(s_i, t_j, p_ji, valid_segs):
    flatten_p_ji = []
    for j in p_ji:
        for i in p_ji[j]:
            flatten_p_ji.append((j,i,p_ji[j][i]))

    sorted_p = sorted(flatten_p_ji, key=lambda kv: kv[2], reverse=True)

    seg_pairs = []
    threshold = 0.01 if len(t_j) < 10000 else 0.3
    max_entry = 1000
    for item in sorted_p:
        j, i, v = item
        if (len(valid_segs) != 0 and i not in valid_segs):
            continue
        v_i = s_i[i]
        v_j = t_j[j]
        if threshold*v_i < v < (1-threshold) * v_i or threshold*v_j < v < (1-threshold) * v_j:
            seg_pairs.append({
                'seg_id': i,
                'gt_id': j,
                'seg_size': s_i[i],
                'gt_size': t_j[j]
            })
            if len(seg_pairs) >= max_entry:
                break

    return seg_pairs
