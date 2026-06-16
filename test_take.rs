fn main() {
    let iter = vec![1, 2, 3].into_iter();
    let taken = iter.take(2);
    let (lower, upper) = taken.size_hint();
    println!("lower: {}, upper: {:?}", lower, upper);
}
