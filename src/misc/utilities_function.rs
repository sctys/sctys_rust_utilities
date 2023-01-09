pub fn get_function_name<F>(_: F) -> &'static str
where
    F: Fn(),
{
    std::any::type_name::<F>()
}

#[macro_export]
macro_rules! function_name {
    ($full_name:literal) => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);

        // Find and cut the rest of the path
        if $full_name {
            &name[..name.len() - 3]
        } else {
            match &name[..name.len() - 3].rfind(':') {
                Some(pos) => &name[pos + 1..name.len() - 3],
                None => &name[..name.len() - 3],
            }
        }
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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_get_function_name() {
        let func_name = get_function_name(test_get_current_function_name);
        assert_eq!(
            func_name,
            "sctys_rust_utilities::misc::utilities_function::tests::test_get_current_function_name"
        )
    }

    #[test]
    fn test_get_current_function_name() {
        let expected_func_name = "test_get_current_function_name";
        let func_name = function_name!(false);
        assert_eq!(expected_func_name, func_name)
    }

    #[test]
    fn test_timeit() {
        fn looping_sum(count: u64) -> u64 {
            let mut total: u64 = 0;
            for i in 1..(count + 1) {
                total += i;
            }
            total
        }
        let num: u64 = 100000;
        let expected_total = num / 2 * (1 + num);
        let cal_total = timeit!(looping_sum(num));
        assert_eq!(expected_total, cal_total)
    }
}
