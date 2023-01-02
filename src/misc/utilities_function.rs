pub fn get_function_name<F>(_: F) -> &'static str
where
    F: Fn(),
{
    std::any::type_name::<F>()
}

#[macro_export]
macro_rules! function_name {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);

        // Find and cut the rest of the path
        let func_name = match &name[..name.len() - 3].rfind(':') {
            Some(pos) => &name[pos + 1..name.len() - 3],
            None => &name[..name.len() - 3],
        };
        func_name
    }};
}

#[macro_export]
macro_rules! timeit {
    ($func_name:ident($($args:expr),*)) => {{
        let _start = std::time::Instant::now();
        let _res = $func_name($($args,)*);
        println!("Running time for {}: {}", stringify!($func_name), _start.elapsed().as_millis());
        _res
    }};
}

pub use function_name;
pub use timeit;