fn main() {
    let mut remaining = 1usize;
    let max = 0xFFFFFFFF_FFFFFFFFu64;
    let min = 0u64;
    // this overflows u64
    let val = max - min + 1;
    println!("val: {}", val);
}
