pub fn get_function_name<F>(_: F) -> &'static str
where
    F: Fn(),
{
    std::any::type_name::<F>()
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

pub use timeit;