use zarrs::array::Array;

fn check_zarrs_methods(array: &Array<dyn zarrs::storage::ReadableStorageTraits>) {
    // try to compile with nonexistent method to see what methods exist
    array.does_not_exist();
}
