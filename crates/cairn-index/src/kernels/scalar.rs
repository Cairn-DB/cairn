//! Scalar reference kernels: the oracle every SIMD variant is tested against.

/// Squared L2 distance.
pub fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len());
    let mut acc = [0f32; 4];
    let mut i = 0;
    while i + 4 <= a.len() {
        for k in 0..4 {
            let d = a[i + k] - b[i + k];
            acc[k] += d * d;
        }
        i += 4;
    }
    let mut s = acc[0] + acc[1] + acc[2] + acc[3];
    while i < a.len() {
        let d = a[i] - b[i];
        s += d * d;
        i += 1;
    }
    s
}

/// Dot product.
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len());
    let mut acc = [0f32; 4];
    let mut i = 0;
    while i + 4 <= a.len() {
        for k in 0..4 {
            acc[k] += a[i + k] * b[i + k];
        }
        i += 4;
    }
    let mut s = acc[0] + acc[1] + acc[2] + acc[3];
    while i < a.len() {
        s += a[i] * b[i];
        i += 1;
    }
    s
}

/// Squared L2 from `query` to each row.
pub fn l2_sq_batch(query: &[f32], rows: &[f32], out: &mut [f32]) {
    let d = query.len();
    for (o, row) in out.iter_mut().zip(rows.chunks_exact(d)) {
        *o = l2_sq(query, row);
    }
}

/// Dot from `query` to each row.
pub fn dot_batch(query: &[f32], rows: &[f32], out: &mut [f32]) {
    let d = query.len();
    for (o, row) in out.iter_mut().zip(rows.chunks_exact(d)) {
        *o = dot(query, row);
    }
}

/// Squared L2 from `query` to each SQ8 row, dequantizing on the fly.
pub fn l2_sq_u8_batch(query: &[f32], rows: &[u8], min: &[f32], scale: &[f32], out: &mut [f32]) {
    let d = query.len();
    assert!(min.len() == d && scale.len() == d);
    for (o, row) in out.iter_mut().zip(rows.chunks_exact(d)) {
        let mut s = 0f32;
        for i in 0..d {
            let x = min[i] + f32::from(row[i]) * scale[i];
            let diff = query[i] - x;
            s += diff * diff;
        }
        *o = s;
    }
}

/// Dot from `query` to each SQ8 row, dequantizing on the fly.
pub fn dot_u8_batch(query: &[f32], rows: &[u8], min: &[f32], scale: &[f32], out: &mut [f32]) {
    let d = query.len();
    assert!(min.len() == d && scale.len() == d);
    for (o, row) in out.iter_mut().zip(rows.chunks_exact(d)) {
        let mut s = 0f32;
        for i in 0..d {
            s += query[i] * (min[i] + f32::from(row[i]) * scale[i]);
        }
        *o = s;
    }
}
