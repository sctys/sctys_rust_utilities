use crate::logger::ProjectLogger;
use redis::{Client, Commands, Connection, RedisResult};

pub struct Redis<'a> {
    project_logger: &'a ProjectLogger,
}

impl<'a> Redis<'a> {
    const REDIS_PATH: &'a str = "redis://127.0.0.1:6379";

    pub fn new(project_logger: &'a ProjectLogger) -> Self {
        Self { project_logger }
    }

    pub fn create_connection(&self) -> RedisResult<Connection> {
        let client = Client::open(Self::REDIS_PATH).unwrap_or_else(|e| {
            let error_str = format!("Fail to build redis connection client. {e}");
            self.project_logger.log_error(&error_str);
            panic!("{}", &error_str);
        });
        let conn = client.get_connection().unwrap_or_else(|e| {
            let error_str = format!("Fail to get redis connection. {e}");
            self.project_logger.log_error(&error_str);
            panic!("{}", &error_str);
        });
        Ok(conn)
    }

    pub fn get_value_from_key(key: &str, conn: &mut Connection) -> usize {
        conn.get(key).unwrap_or(0i32) as usize
    }

    pub fn reset_key(&self, key: &str, conn: &mut Connection) {
        conn.del::<_, i32>(key).unwrap_or_else(|e| {
            let error_str = format!("Fail to remove redis key {key}. {e}");
            self.project_logger.log_error(&error_str);
            panic!("{}", &error_str);
        });
    }
}

#[cfg(test)]
mod tests {

    use std::{env, path::Path};

    use log::LevelFilter;

    use super::*;

    #[test]
    fn test_get_value_from_key() {
        let logger_name = "test_duck_db";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let redis = Redis::new(&project_logger);
        let key = "oddsportal_competition_season";
        let mut conn = redis.create_connection().unwrap();
        let value = Redis::get_value_from_key(key, &mut conn);
        dbg!(value);
    }

    #[test]
    fn test_reset_key() {
        let logger_name = "test_duck_db";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        project_logger.set_logger(LevelFilter::Debug);
        let redis = Redis::new(&project_logger);
        let key = "test";
        let mut conn = redis.create_connection().unwrap();
        conn.set::<&str, i32, ()>(key, 11i32).unwrap();
        let value = Redis::get_value_from_key(key, &mut conn);
        assert_eq!(value, 11);
        redis.reset_key(key, &mut conn);
        let value = Redis::get_value_from_key(key, &mut conn);
        assert_eq!(value, 0);
    }
}
