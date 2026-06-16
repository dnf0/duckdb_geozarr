fn main() {
    let mut iter = (0..100).filter(|x| x % 2 == 0).take(50);
    println!("{:?}", iter.size_hint());
    let v: Vec<i32> = iter.collect();
    println!("capacity: {}", v.capacity());
}
