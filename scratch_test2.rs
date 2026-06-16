fn main() {
    let mut v = Vec::with_capacity(100);
    v.extend((0..10).filter(|x| x % 2 == 0));
    println!("capacity: {}", v.capacity());
}
