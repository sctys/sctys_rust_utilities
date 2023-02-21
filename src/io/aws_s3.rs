use crate::logger::ProjectLogger;
use crate::time_operation;
use crate::time_operation::SecPrecision;
use aws_sdk_s3::model::Object;
use aws_sdk_s3::output::ListObjectsV2Output;
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Credentials, Region};
use aws_smithy_http::body::SdkBody;
use chrono::{DateTime, TimeZone};
use polars::frame::DataFrame;
use polars::prelude::{CsvReader, CsvWriter, ParquetReader, ParquetWriter};
use polars_io::{SerReader, SerWriter};
use serde::Deserialize;
use std::env;
use std::fs;
use std::io::Cursor;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use toml;

#[derive(Debug, Clone)]
pub struct AWSFileIO<'a> {
    project_logger: &'a ProjectLogger,
    client: Client,
}

impl<'a> AWSFileIO<'a> {
    pub async fn new(project_logger: &'a ProjectLogger) -> AWSFileIO {
        let api_key = APIKey::load_apikey();
        let credentials = Credentials::new(
            &api_key.aws_api_id,
            &api_key.aws_api_secret,
            None,
            None,
            "s3_access",
        );
        let region = Region::new(api_key.aws_api_region.clone());
        let config = aws_config::from_env()
            .credentials_provider(credentials)
            .region(region)
            .load()
            .await;
        let client = Client::new(&config);
        Self {
            project_logger,
            client,
        }
    }

    pub async fn check_bucket_exist(&self, bucket_name: &String) -> bool {
        self.client
            .head_bucket()
            .bucket(bucket_name)
            .send()
            .await
            .is_ok()
    }

    pub async fn check_folder_exist(&self, bucket_name: &String, folder_name: &String) -> bool {
        self.client
            .head_object()
            .bucket(bucket_name)
            .key(folder_name)
            .send()
            .await
            .is_ok()
    }

    pub async fn check_file_exist(
        &self,
        bucket_name: &String,
        folder_name: &String,
        file_name: &String,
    ) -> bool {
        let full_path = format!("{folder_name}{file_name}");
        self.client
            .head_object()
            .bucket(bucket_name)
            .key(full_path)
            .send()
            .await
            .is_ok()
    }

    pub async fn create_directory_if_not_exists(&self, bucket_name: &String, folder_name: &String) {
        if !self.check_folder_exist(bucket_name, folder_name).await {
            match self
                .client
                .put_object()
                .bucket(bucket_name)
                .key(folder_name)
                .send()
                .await
            {
                Ok(_) => {
                    let debug_str = format!("Folder {folder_name} created in bucket {bucket_name}");
                    self.project_logger.log_debug(&debug_str);
                }
                Err(e) => {
                    let error_str = format!(
                        "Unable to create folder {folder_name} in bucket {bucket_name}. {e}"
                    );
                    self.project_logger.log_error(&error_str);
                }
            }
        }
    }

    pub async fn get_elements_in_folder(
        &self,
        bucket_name: &String,
        folder_name: &String,
    ) -> ListObjectsV2Output {
        match self
            .client
            .list_objects_v2()
            .bucket(bucket_name)
            .prefix(folder_name)
            .send()
            .await
        {
            Ok(object_list) => object_list,
            Err(e) => {
                let error_str =
                    format!("Unable to get the list of file in folder {folder_name}, {e}");
                self.project_logger.log_error(&error_str);
                panic!("{error_str}")
            }
        }
    }

    pub fn filter_element_after<T: TimeZone>(
        &self,
        element: &Object,
        cutoff_date_time: DateTime<T>,
    ) -> bool {
        let cutoff_timestamp =
            time_operation::date_time_to_timestamp(cutoff_date_time, SecPrecision::Sec);
        let last_modified_time = element.last_modified();
        last_modified_time.map_or(false, |last_modified| {
            last_modified.secs() >= cutoff_timestamp
        })
    }

    pub fn filter_element_between<T: TimeZone>(
        &self,
        element: &Object,
        cutoff_date_time_early: DateTime<T>,
        cutoff_date_time_late: DateTime<T>,
    ) -> bool {
        let cutoff_timestamp_early =
            time_operation::date_time_to_timestamp(cutoff_date_time_early, SecPrecision::Sec);
        let cutoff_timestamp_late =
            time_operation::date_time_to_timestamp(cutoff_date_time_late, SecPrecision::Sec);
        let last_modified_time = element.last_modified();
        last_modified_time.map_or(false, |last_modified| {
            last_modified.secs() >= cutoff_timestamp_early
                && last_modified.secs() < cutoff_timestamp_late
        })
    }

    pub async fn load_file_as_string(
        &self,
        bucket_name: &String,
        folder_path: &String,
        file: &String,
    ) -> String {
        let full_path = format!("{folder_path}{file}");
        match self
            .client
            .get_object()
            .bucket(bucket_name)
            .key(&full_path)
            .send()
            .await
        {
            Ok(get_object) => match get_object.body.collect().await {
                Ok(byte) => String::from_utf8_lossy(&byte.to_vec()).to_string(),
                Err(e) => {
                    let error_str = format!("Unable to read the file {file} from folder {folder_path} in bucket {bucket_name}. {e}");
                    self.project_logger.log_error(&error_str);
                    panic!("{error_str}");
                }
            },
            Err(e) => {
                let error_str = format!("Unable to get the file {file} from folder {folder_path} in bucket {bucket_name}. {e}");
                self.project_logger.log_error(&error_str);
                panic!("{error_str}");
            }
        }
    }

    pub async fn write_string_to_file(
        &self,
        bucket_name: &String,
        folder_path: &String,
        file: &String,
        content: &str,
    ) {
        let full_path = format!("{folder_path}{file}");
        let content_byte = ByteStream::new(SdkBody::from(content));
        match self
            .client
            .put_object()
            .bucket(bucket_name)
            .key(&full_path)
            .body(content_byte)
            .send()
            .await
        {
            Ok(_) => {
                let debug_str = format!("File {full_path} saved in bucket {bucket_name}");
                self.project_logger.log_debug(&debug_str);
            }
            Err(e) => {
                let error_str = format!("Unable to save {full_path} in bucket {bucket_name}, {e}");
                self.project_logger.log_error(&error_str);
                panic!("{error_str}")
            }
        }
    }

    pub async fn load_csv_file(
        &self,
        bucket_name: &String,
        folder_path: &String,
        file: &String,
    ) -> DataFrame {
        let full_path = format!("{folder_path}{file}");
        match self
            .client
            .get_object()
            .bucket(bucket_name)
            .key(&full_path)
            .send()
            .await
        {
            Ok(get_object) => match get_object.body.collect().await {
                Ok(byte) => {
                    let cursor = Cursor::new(byte.into_bytes());
                    match CsvReader::new(cursor).has_header(true).finish() {
                        Ok(df) => df,
                        Err(e) => {
                            let error_str = format!("Unable to convert the bytes from file {file} from folder {folder_path} in bucket {bucket_name} into data frame. {e}");
                            self.project_logger.log_error(&error_str);
                            panic!("{error_str}");
                        }
                    }
                }
                Err(e) => {
                    let error_str = format!("Unable to read the file {file} from folder {folder_path} in bucket {bucket_name}. {e}");
                    self.project_logger.log_error(&error_str);
                    panic!("{error_str}");
                }
            },
            Err(e) => {
                let error_str = format!("Unable to get the file {file} from folder {folder_path} in bucket {bucket_name}. {e}");
                self.project_logger.log_error(&error_str);
                panic!("{error_str}");
            }
        }
    }

    pub async fn write_csv_file(
        &self,
        bucket_name: &String,
        folder_path: &String,
        file: &String,
        data: &mut DataFrame,
    ) {
        let full_path = format!("{folder_path}{file}");
        let mut buffer = Vec::new();
        let cursor = Cursor::new(&mut buffer);
        let csv_writer = CsvWriter::new(cursor);
        match csv_writer
            .has_header(true)
            .with_delimiter(b',')
            .finish(data)
        {
            Ok(_) => {
                let csv_string = ByteStream::from(buffer);
                match self
                    .client
                    .put_object()
                    .bucket(bucket_name)
                    .key(&full_path)
                    .body(csv_string)
                    .send()
                    .await
                {
                    Ok(_) => {
                        let debug_str = format!("File {full_path} saved in bucket {bucket_name}");
                        self.project_logger.log_debug(&debug_str);
                    }
                    Err(e) => {
                        let error_str =
                            format!("Unable to save {full_path} in bucket {bucket_name}, {e}");
                        self.project_logger.log_error(&error_str);
                        panic!("{error_str}");
                    }
                }
            }
            Err(e) => {
                let error_str = format!("Unable to convert {full_path} into bytestream. {e}");
                self.project_logger.log_error(&error_str);
                panic!("{error_str}");
            }
        }
    }

    pub async fn load_parquet_file(
        &self,
        bucket_name: &String,
        folder_path: &String,
        file: &String,
    ) -> DataFrame {
        let full_path = format!("{folder_path}{file}");
        match self
            .client
            .get_object()
            .bucket(bucket_name)
            .key(&full_path)
            .send()
            .await
        {
            Ok(get_object) => match get_object.body.collect().await {
                Ok(byte) => {
                    let cursor = Cursor::new(byte.into_bytes());
                    match ParquetReader::new(cursor).finish() {
                        Ok(df) => df,
                        Err(e) => {
                            let error_str = format!("Unable to convert the bytes from file {file} from folder {folder_path} in bucket {bucket_name} into data frame. {e}");
                            self.project_logger.log_error(&error_str);
                            panic!("{error_str}");
                        }
                    }
                }
                Err(e) => {
                    let error_str = format!("Unable to read the file {file} from folder {folder_path} in bucket {bucket_name}. {e}");
                    self.project_logger.log_error(&error_str);
                    panic!("{error_str}")
                }
            },
            Err(e) => {
                let error_str = format!("Unable to get the file {file} from folder {folder_path} in bucket {bucket_name}. {e}");
                self.project_logger.log_error(&error_str);
                panic!("{error_str}")
            }
        }
    }

    pub async fn write_parquet_file(
        &self,
        bucket_name: &String,
        folder_path: &String,
        file: &String,
        data: &mut DataFrame,
    ) {
        let full_path = format!("{folder_path}{file}");
        let mut buffer = Vec::new();
        let cursor = Cursor::new(&mut buffer);
        let parquet_writer = ParquetWriter::new(cursor);
        match parquet_writer.finish(data) {
            Ok(_) => {
                let parquet_string = ByteStream::from(buffer);
                match self
                    .client
                    .put_object()
                    .bucket(bucket_name)
                    .key(&full_path)
                    .body(parquet_string)
                    .send()
                    .await
                {
                    Ok(_) => {
                        let debug_str = format!("File {full_path} saved in bucket {bucket_name}");
                        self.project_logger.log_debug(&debug_str);
                    }
                    Err(e) => {
                        let error_str =
                            format!("Unable to save {full_path} in bucket {bucket_name}, {e}");
                        self.project_logger.log_error(&error_str);
                        panic!("{error_str}")
                    }
                }
            }
            Err(e) => {
                let error_str = format!("Unable to convert {full_path} into bytestream. {e}");
                self.project_logger.log_error(&error_str);
                panic!("{error_str}")
            }
        }
    }

    pub async fn download_file(
        &self,
        bucket_name: &String,
        folder_path: &String,
        file: &String,
        local_path: &Path,
        local_file: &String,
    ) {
        let full_path = format!("{folder_path}{file}");
        match self
            .client
            .get_object()
            .bucket(bucket_name)
            .key(&full_path)
            .send()
            .await
        {
            Ok(get_object) => match get_object.body.collect().await {
                Ok(byte) => {
                    let full_local_path = local_path.join(local_file);
                    match tokio::fs::write(&full_local_path, byte.to_vec()).await {
                        Ok(_) => {
                            let debug_str = format!("File {} downloaded.", &full_path);
                            self.project_logger.log_debug(&debug_str);
                        }
                        Err(e) => {
                            let error_str =
                                format!("Unable to download to file {}. {e}", &full_path);
                            self.project_logger.log_error(&error_str);
                            panic!("{}", &error_str);
                        }
                    };
                }
                Err(e) => {
                    let error_str = format!("Unable to read the file {file} from folder {folder_path} in bucket {bucket_name}. {e}");
                    self.project_logger.log_error(&error_str);
                    panic!("{error_str}");
                }
            },
            Err(e) => {
                let error_str = format!("Unable to get the file {file} from folder {folder_path} in bucket {bucket_name}. {e}");
                self.project_logger.log_error(&error_str);
                panic!("{error_str}");
            }
        }
    }

    pub async fn upload_file(
        &self,
        bucket_name: &String,
        folder_path: &String,
        file: &String,
        local_path: &Path,
        local_file: &String,
    ) {
        let full_local_path = local_path.join(local_file);
        match File::open(&full_local_path).await {
            Ok(mut temp_file) => {
                let mut bytes = Vec::new();
                match temp_file.read_to_end(&mut bytes).await {
                    Ok(_) => {
                        let content = ByteStream::from(bytes);
                        let full_path = format!("{folder_path}{file}");
                        match self
                            .client
                            .put_object()
                            .bucket(bucket_name)
                            .key(&full_path)
                            .body(content)
                            .send()
                            .await
                        {
                            Ok(_) => {
                                let debug_str = format!("File {} uploaded.", &full_path);
                                self.project_logger.log_debug(&debug_str);
                            }
                            Err(e) => {
                                let error_str =
                                    format!("Unable to upload to file {}. {e}", &full_path);
                                self.project_logger.log_error(&error_str);
                                panic!("{}", &error_str);
                            }
                        }
                    }
                    Err(e) => {
                        let error_str = format!(
                            "Unable to red the local file {} as bytes. {e}",
                            &full_local_path.display()
                        );
                        self.project_logger.log_error(&error_str);
                        panic!("{error_str}");
                    }
                }
            }
            Err(e) => {
                let error_str = format!(
                    "Unable to open the local file {}. {e}",
                    &full_local_path.display()
                );
                self.project_logger.log_error(&error_str);
                panic!("{error_str}");
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct APIKey {
    aws_api_id: String,
    aws_api_secret: String,
    aws_api_region: String,
}

impl APIKey {
    const PROJECT_KEY: &str = "SCTYS_PROJECT";
    const API_KEY_PATH: &str = "Secret/secret_sctys_rust_utilities";
    const API_KEY_FILE: &str = "aws_s3_api.toml";

    fn load_apikey() -> APIKey {
        let full_api_path =
            Path::new(&env::var(Self::PROJECT_KEY).expect("Unable to find project path"))
                .join(Self::API_KEY_PATH)
                .join(Self::API_KEY_FILE);
        let api_str = match fs::read_to_string(full_api_path) {
            Ok(api_str) => api_str,
            Err(e) => panic!("Unable to load the api file. {e}"),
        };
        let api_key_data: APIKey = match toml::from_str(&api_str) {
            Ok(api_data) => api_data,
            Err(e) => panic!("Unable to parse the api file. {e}"),
        };
        api_key_data
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::file_io::FileIO;
    use log::LevelFilter;

    #[tokio::test]
    async fn test_check_bucket_exist() {
        let logger_name = "test_aws_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let aws_file_io = AWSFileIO::new(&project_logger).await;
        let bucket_name = "sctys".to_string();
        assert!(aws_file_io.check_bucket_exist(&bucket_name).await);
        let bucket_name = "abc".to_string();
        assert!(!aws_file_io.check_bucket_exist(&bucket_name).await);
    }

    #[tokio::test]
    async fn test_check_folder_exist() {
        let logger_name = "test_aws_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let aws_file_io = AWSFileIO::new(&project_logger).await;
        let bucket_name = "sctys".to_string();
        let folder_name = "data/poisson_football/".to_string();
        assert!(
            aws_file_io
                .check_folder_exist(&bucket_name, &folder_name)
                .await
        );
        let folder_name = "abc".to_string();
        assert!(
            !aws_file_io
                .check_folder_exist(&bucket_name, &folder_name)
                .await
        );
    }

    #[tokio::test]
    async fn test_check_file_exist() {
        let logger_name = "test_aws_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let aws_file_io = AWSFileIO::new(&project_logger).await;
        let bucket_name = "sctys".to_string();
        let folder_name = "data/poisson_football/".to_string();
        let file_name = "test_list.html".to_string();
        assert!(
            aws_file_io
                .check_file_exist(&bucket_name, &folder_name, &file_name)
                .await
        );
        let file_name = "abc".to_string();
        assert!(
            !aws_file_io
                .check_file_exist(&bucket_name, &folder_name, &file_name)
                .await
        );
    }

    #[tokio::test]
    async fn test_create_directory() {
        let logger_name = "test_aws_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let aws_file_io = AWSFileIO::new(&project_logger).await;
        let bucket_name = "sctys".to_string();
        let folder_name = "data/test_folder/".to_string();
        aws_file_io
            .create_directory_if_not_exists(&bucket_name, &folder_name)
            .await;
    }

    #[tokio::test]
    async fn test_get_element_from_folder() {
        let logger_name = "test_aws_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let aws_file_io = AWSFileIO::new(&project_logger).await;
        let bucket_name = "sctys".to_string();
        let folder_name = "data/poisson_football/".to_string();
        let elements = aws_file_io
            .get_elements_in_folder(&bucket_name, &folder_name)
            .await;
        println!("{:?}", elements.contents().unwrap());
    }

    #[tokio::test]
    async fn test_read_file_as_string() {
        let logger_name = "test_aws_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let aws_file_io = AWSFileIO::new(&project_logger).await;
        let bucket_name = "sctys".to_string();
        let folder_name = "data/poisson_football/".to_string();
        let file_name = "test_list.html".to_string();
        let content = aws_file_io
            .load_file_as_string(&bucket_name, &folder_name, &file_name)
            .await;
        println!("{:?}", content);
    }

    #[tokio::test]
    async fn test_write_string_as_file() {
        let logger_name = "test_aws_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let aws_file_io = AWSFileIO::new(&project_logger).await;
        let local_folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let local_file = "test.html".to_owned();
        let file_io = FileIO::new(&project_logger);
        let data = file_io.load_file_as_string(&local_folder_path, &local_file);
        let bucket_name = "sctys".to_string();
        let folder_name = "data/test_folder/".to_string();
        let file_name = "test_aws.html".to_string();
        aws_file_io
            .write_string_to_file(&bucket_name, &folder_name, &file_name, data.as_str())
            .await;
    }

    #[tokio::test]
    async fn test_load_csv_file() {
        let logger_name = "test_aws_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let aws_file_io = AWSFileIO::new(&project_logger).await;
        let bucket_name = "sctys".to_string();
        let folder_name = "data/test_folder/".to_string();
        let file_name = "test.csv".to_string();
        let content = aws_file_io
            .load_csv_file(&bucket_name, &folder_name, &file_name)
            .await;
        println!("{:?}", content);
    }

    #[tokio::test]
    async fn test_write_csv_file() {
        let logger_name = "test_aws_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let file_io = FileIO::new(&project_logger);
        let aws_file_io = AWSFileIO::new(&project_logger).await;
        let local_folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let local_file = "test_new.csv".to_owned();
        let mut data = file_io.load_csv_file(&local_folder_path, &local_file);
        let bucket_name = "sctys".to_string();
        let folder_name = "data/test_folder/".to_string();
        let file_name = "test_new_aws.csv".to_string();
        aws_file_io
            .write_csv_file(&bucket_name, &folder_name, &file_name, &mut data)
            .await;
    }

    #[tokio::test]
    async fn test_parquet_file() {
        let logger_name = "test_aws_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let file_io = FileIO::new(&project_logger);
        let aws_file_io = AWSFileIO::new(&project_logger).await;
        let local_folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let local_file = "test.parquet".to_owned();
        let mut data = file_io.load_parquet_file(&local_folder_path, &local_file);
        let bucket_name = "sctys".to_string();
        let folder_name = "data/test_folder/".to_string();
        let file_name = "test_aws.parquet".to_string();
        aws_file_io
            .write_parquet_file(&bucket_name, &folder_name, &file_name, &mut data)
            .await;
        let content = aws_file_io
            .load_parquet_file(&bucket_name, &folder_name, &file_name)
            .await;
        println!("{:?}", content);
    }

    #[tokio::test]
    async fn test_download_file() {
        let logger_name = "test_aws_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let aws_file_io = AWSFileIO::new(&project_logger).await;
        let local_folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let local_file = "test_aws.parquet".to_owned();
        let bucket_name = "sctys".to_string();
        let folder_name = "data/test_folder/".to_string();
        let file_name = "test_aws.parquet".to_string();
        aws_file_io
            .download_file(
                &bucket_name,
                &folder_name,
                &file_name,
                &local_folder_path,
                &local_file,
            )
            .await;
    }

    #[tokio::test]
    async fn test_upload_file() {
        let logger_name = "test_aws_file_io";
        let logger_path = Path::new(&env::var("SCTYS_PROJECT").unwrap())
            .join("Log")
            .join("log_sctys_io");
        let project_logger = ProjectLogger::new_logger(&logger_path, logger_name);
        let _handle = project_logger.set_logger(LevelFilter::Debug);
        let aws_file_io = AWSFileIO::new(&project_logger).await;
        let local_folder_path = Path::new(&env::var("SCTYS_DATA").unwrap()).join("test_io");
        let local_file = "test_scrape.html".to_owned();
        let bucket_name = "sctys".to_string();
        let folder_name = "data/test_folder/".to_string();
        let file_name = "test_scrape_aws.html".to_string();
        aws_file_io
            .upload_file(
                &bucket_name,
                &folder_name,
                &file_name,
                &local_folder_path,
                &local_file,
            )
            .await;
    }
}
