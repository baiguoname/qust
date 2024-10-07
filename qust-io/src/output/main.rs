use ndarray::arr1;

fn main() {
    let v = vec![1f32; 100_000_000];
    let a = arr1(&v);

    let time_start = std::time::Instant::now();
    let k1 = v.iter().zip(v.iter()).map(|(x, y)| x + y);
    let k2 = k1.zip(v.iter()).map(|(x, y)| x - y);
    let res = k2.collect::<Vec<f32>>();
    println!("{:?}, {:?}", time_start.elapsed(), res.len());

    let time_start = std::time::Instant::now();
    let k1 = &a + &a;
    let res = k1 - &a;
    println!("{:?}, {:?}", time_start.elapsed(), res.len());
}

