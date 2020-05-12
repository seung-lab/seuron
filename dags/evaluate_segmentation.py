def read_chunk(f, s_i, t_j, p_ji):
    import struct
    try:
        count, = struct.unpack('q', f.read(8))
    except:
        return False

    for i in range(count):
        seg, vx = struct.unpack('qq', f.read(16))
        s_i[seg] += vx

    count, = struct.unpack('q', f.read(8))
    for i in range(count):
        seg, vx = struct.unpack('qq', f.read(16))
        t_j[seg] += vx

    count, = struct.unpack('q', f.read(8))
    for i in range(count):
        s1, s2, vx = struct.unpack('qqq', f.read(24))
        p_ji[s1][s2] += vx

    return True


def evaluate_rand(s_i, t_j, p_ji):
    sum_p_ji_square = 0
    for j in p_ji:
        for i in p_ji[j]:
            sum_p_ji_square += p_ji[j][i]*p_ji[j][i]

    sum_s_i_square = 0
    for i in s_i:
        sum_s_i_square += s_i[i]*s_i[i]

    sum_t_j_square = 0
    for j in t_j:
        sum_t_j_square += t_j[j]*t_j[j]

    rand_split = sum_p_ji_square/sum_t_j_square
    rand_merge = sum_p_ji_square/sum_s_i_square

    return rand_split, rand_merge


def evaluate_voi(s_i, t_j, p_ji):
    from math import log
    total = 0
    for i in s_i:
        total += s_i[i]

    H_st = 0
    for j in p_ji:
        for i in p_ji[j]:
            H_st -= p_ji[j][i]/total * log(p_ji[j][i]/total)

    H_s = 0
    for i in s_i:
        H_s -= s_i[i]/total * log(s_i[i]/total)

    H_t = 0
    for j in t_j:
        H_t -= t_j[j]/total * log(t_j[j]/total)

    voi_split = H_st - H_t
    voi_merge = H_st - H_s

    return voi_split, voi_merge

def find_large_diff(s_i, t_j, p_ji, param):
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
