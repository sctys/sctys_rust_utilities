use crate::logger::ProjectLogger;
use crate::time_operation;
use crate::time_operation::SecPrecision;
use aws_sdk_s3::error::{
    CompleteMultipartUploadError, CreateMultipartUploadError, GetObjectError, ListObjectsV2Error,
    PutObjectError, UploadPartError,
};
use aws_sdk_s3::model::{CompletedMultipartUpload, CompletedPart, Object};
use aws_sdk_s3::output::ListObjectsV2Output;
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Credentials, Region};
use aws_smithy_http::body::SdkBody;
use aws_smithy_http::result::SdkError;
use chrono::{DateTime, TimeZone};
use polars::error::PolarsError;
use polars::frame::DataFrame;
use polars::io::{SerReader, SerWriter};
use polars::prelude::{CsvReadOptions, CsvWriter, ParquetReader, ParquetWriter};
use serde::Deserialize;
use std::env;
use std::fs;
use std::io::{Cursor, SeekFrom};
use std::path::{Path, PathBuf};
use std::result::Result;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use toml;

const MULTIPART_SIZE: usize = 1024 * 1024 * 1024; // 1GB per part
const LIMIT_SINGLE_UPLOAD: usize = 5 * MULTIPART_SIZE;

#[derive(Debug, Clone)]
pub struct AWSFileIO<'a> {
    project_logger: &'a ProjectLogger,
    client: Client,
}

impl<'a> AWSFileIO<'a> {
    const MAX_KEY: i32 = 100;

    pub async fn new(project_logger: &'a ProjectLogger) -> AWSFileIO<'a> {
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

    fn add_stash_for_folder_suffix(folder_name: &Path) -> PathBuf {
        if folder_name
            .to_string_lossy()
            .chars()
            .last()
            .unwrap_or_else(|| panic!("Folder name is empty"))
            != '/'
        {
            folder_name.join("")
        } else {
            folder_name.to_path_buf()
        }
    }

    pub async fn check_bucket_exist(&self, bucket_name: &str) -> bool {
        self.client
            .head_bucket()
            .bucket(bucket_name)
            .send()
            .await
            .is_ok()
    }

    pub async fn check_folder_exist(&self, bucket_name: &str, folder_name: &Path) -> bool {
        self.client
            .head_object()
            .bucket(bucket_name)
            .key(Self::add_stash_for_folder_suffix(folder_name).to_string_lossy())
            .send()
            .await
            .is_ok()
    }

    pub async fn check_file_exist(
        &self,
        bucket_name: &str,
        folder_name: &Path,
        file_name: &str,
    ) -> bool {
        let full_path = folder_name.join(file_name);
        self.client
            .head_object()
            .bucket(bucket_name)
            .key(full_path.to_string_lossy())
            .send()
            .await
            .is_ok()
    }

    pub async fn create_directory_if_not_exists(
        &self,
        bucket_name: &str,
        folder_name: &Path,
    ) -> Result<(), SdkError<PutObjectError>> {
        if !self.check_folder_exist(bucket_name, folder_name).await {
            self.client
                .put_object()
                .bucket(bucket_name)
                .key(Self::add_stash_for_folder_suffix(folder_name).to_string_lossy())
                .send()
                .await
                .map_or_else(
                    |e| {
                        let error_str = format!(
                            "Unable to create folder {} in bucket {bucket_name}. {e}",
                            folder_name.display()
                        );
                        self.project_logger.log_error(&error_str);
                        Err(e)
                    },
                    |_| {
                        let debug_str = format!(
                            "Folder {} created in bucket {bucket_name}",
                            folder_name.display()
                        );
                        self.project_logger.log_debug(&debug_str);
                        Ok(())
                    },
                )
        } else {
            let error_str = format!(
                "Folder {} already exists in bucket {bucket_name}.",
                folder_name.display()
            );
            self.project_logger.log_error(&error_str);
            Err(SdkError::construction_failure(error_str))
        }
    }

    pub async fn get_elements_in_folder(
        &self,
        bucket_name: &str,
        folder_name: &Path,
    ) -> Result<Vec<ListObjectsV2Output>, SdkError<ListObjectsV2Error>> {
        let mut object_output_list = Vec::new();
        let mut is_last_page = false;
        let mut continuation_token = None;
        while !is_last_page {
            match self
                .client
                .list_objects_v2()
                .bucket(bucket_name)
                .prefix(Self::add_stash_for_folder_suffix(folder_name).to_string_lossy())
                .set_continuation_token(continuation_token)
                .max_keys(Self::MAX_KEY)
                .send()
                .await
            {
                Ok(object_list) => {
                    continuation_token = object_list.next_continuation_token().map(str::to_string);
                    if !object_list.is_truncated() {
                        is_last_page = true;
                    }
                    object_output_list.push(object_list);
                }
                Err(e) => {
                    let error_str = format!(
                        "Unable to get the list of file in folder {}, {e}",
                        folder_name.display()
                    );
                    self.project_logger.log_error(&error_str);
                    return Err(e);
                }
            }
        }
        Ok(object_output_list)
    }

    pub fn filter_element_after<T: TimeZone>(
        &self,
        element: &Object,
        cutoff_date_time: &DateTime<T>,
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
        cutoff_date_time_early: &DateTime<T>,
        cutoff_date_time_late: &DateTime<T>,
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
        bucket_name: &str,
        folder_path: &Path,
        file: &str,
    ) -> Result<String, AWSLoadFileError> {
        let full_path = folder_path.join(file);
        let get_object = self
            .client
            .get_object()
            .bucket(bucket_name)
            .key(full_path.to_string_lossy())
            .send()
            .await
            .map_err(|e| {
                let error_str = format!(
                    "Unable to get the file {file} from folder {} in bucket {bucket_name}. {e}",
                    folder_path.display()
                );
                self.project_logger.log_error(&error_str);
                AWSLoadFileError::SdkError(e)
            });
        get_object?.body.collect().await.map_or_else(
            |e| {
                let error_str = format!(
                    "Unable to read the file {file} from folder {} in bucket {bucket_name}. {e}",
                    folder_path.display()
                );
                self.project_logger.log_error(&error_str);
                Err(AWSLoadFileError::ByteStreamError(e))
            },
            |byte| {
                let debug_str = format!(
                    "File {file} from folder {} in bucket {bucket_name} loaded.",
                    folder_path.display()
                );
                self.project_logger.log_debug(&debug_str);
                Ok(String::from_utf8_lossy(&byte.to_vec()).to_string())
            },
        )
    }

    pub async fn write_string_to_file(
        &self,
        bucket_name: &str,
        folder_path: &Path,
        file: &str,
        content: &str,
    ) -> Result<(), SdkError<PutObjectError>> {
        let full_path = folder_path.join(file);
        let content_byte = ByteStream::new(SdkBody::from(content));
        self.client
            .put_object()
            .bucket(bucket_name)
            .key(full_path.to_string_lossy())
            .body(content_byte)
            .send()
            .await
            .map_or_else(
                |e| {
                    let error_str = format!(
                        "Unable to save {} in bucket {bucket_name}, {e}",
                        full_path.display()
                    );
                    self.project_logger.log_error(&error_str);
                    Err(e)
                },
                |_| {
                    let debug_str =
                        format!("File {} saved in bucket {bucket_name}", full_path.display());
                    self.project_logger.log_debug(&debug_str);
                    Ok(())
                },
            )
    }

    pub async fn load_csv_file(
        &self,
        bucket_name: &str,
        folder_path: &Path,
        file: &str,
    ) -> Result<DataFrame, AWSLoadFileError> {
        let full_path = folder_path.join(file);
        let get_object = self
            .client
            .get_object()
            .bucket(bucket_name)
            .key(full_path.to_string_lossy())
            .send()
            .await
            .map_err(|e| {
                let error_str = format!(
                    "Unable to get the file {file} from folder {} in bucket {bucket_name}. {e}",
                    folder_path.display()
                );
                self.project_logger.log_error(&error_str);
                AWSLoadFileError::SdkError(e)
            });
        let byte = get_object?.body.collect().await.map_err(|e| {
            let error_str = format!(
                "Unable to read the file {file} from folder {} in bucket {bucket_name}. {e}",
                folder_path.display()
            );
            self.project_logger.log_error(&error_str);
            AWSLoadFileError::ByteStreamError(e)
        });
        let cursor = Cursor::new(byte?.into_bytes());
        CsvReadOptions::default().with_has_header(true).into_reader_with_file_handle(cursor).finish().map_or_else(|e| {
                let error_str = format!("Unable to convert the bytes from file {file} from folder {} in bucket {bucket_name} into data frame. {e}", folder_path.display());
                self.project_logger.log_error(&error_str);
                Err(AWSLoadFileError::PolarsError(e))
            }, |data_frame| {
                let debug_str = format!("File {file} from folder {} in bucket {bucket_name} loaded.", folder_path.display());
                self.project_logger.log_debug(&debug_str);
                Ok(data_frame)
            })
    }

    pub async fn write_csv_file(
        &self,
        bucket_name: &str,
        folder_path: &Path,
        file: &str,
        data: &mut DataFrame,
    ) -> Result<(), AWSWriteFileError> {
        let full_path = folder_path.join(file);
        let mut buffer = Vec::new();
        let cursor = Cursor::new(&mut buffer);
        let csv_writer = CsvWriter::new(cursor);
        if let Err(e) = csv_writer
            .include_header(true)
            .with_separator(b',')
            .finish(data)
        {
            let error_str = format!(
                "Unable to convert {} into bytestream. {e}",
                full_path.display()
            );
            self.project_logger.log_error(&error_str);
            return Err(AWSWriteFileError::PolarsError(e));
        };
        let csv_string = ByteStream::from(buffer);
        self.client
            .put_object()
            .bucket(bucket_name)
            .key(full_path.to_string_lossy())
            .body(csv_string)
            .send()
            .await
            .map_or_else(
                |e| {
                    let error_str = format!(
                        "Unable to save {} in bucket {bucket_name}, {e}",
                        full_path.display()
                    );
                    self.project_logger.log_error(&error_str);
                    Err(AWSWriteFileError::SdkError(e))
                },
                |_| {
                    let debug_str =
                        format!("File {} saved in bucket {bucket_name}", full_path.display());
                    self.project_logger.log_debug(&debug_str);
                    Ok(())
                },
            )
    }

    pub async fn load_parquet_file(
        &self,
        bucket_name: &str,
        folder_path: &Path,
        file: &str,
    ) -> Result<DataFrame, AWSLoadFileError> {
        let full_path = folder_path.join(file);
        let get_object = self
            .client
            .get_object()
            .bucket(bucket_name)
            .key(full_path.to_string_lossy())
            .send()
            .await
            .map_err(|e| {
                let error_str = format!(
                    "Unable to get the file {file} from folder {} in bucket {bucket_name}. {e}",
                    folder_path.display()
                );
                self.project_logger.log_error(&error_str);
                AWSLoadFileError::SdkError(e)
            });
        let byte = get_object?.body.collect().await.map_err(|e| {
            let error_str = format!(
                "Unable to read the file {file} from folder {} in bucket {bucket_name}. {e}",
                folder_path.display()
            );
            self.project_logger.log_error(&error_str);
            AWSLoadFileError::ByteStreamError(e)
        });
        let cursor = Cursor::new(byte?.into_bytes());
        ParquetReader::new(cursor).finish().map_or_else(|e| {
            let error_str = format!("Unable to convert the bytes from file {file} from folder {} in bucket {bucket_name} into data frame. {e}", folder_path.display());
            self.project_logger.log_error(&error_str);
            Err(AWSLoadFileError::PolarsError(e))
        }, |data_frame| {
            let debug_str = format!("File {file} from folder {} in bucket {bucket_name} loaded.", folder_path.display());
            self.project_logger.log_debug(&debug_str);
            Ok(data_frame)
        })
    }

    pub async fn write_parquet_file(
        &self,
        bucket_name: &str,
        folder_path: &Path,
        file: &str,
        data: &mut DataFrame,
    ) -> Result<(), AWSWriteFileError> {
        let full_path = folder_path.join(file);
        let mut buffer = Vec::new();
        let cursor = Cursor::new(&mut buffer);
        let parquet_writer = ParquetWriter::new(cursor);
        if let Err(e) = parquet_writer.finish(data) {
            let error_str = format!(
                "Unable to convert {} into bytestream. {e}",
                full_path.display()
            );
            self.project_logger.log_error(&error_str);
            return Err(AWSWriteFileError::PolarsError(e));
        };
        let parquet_string = ByteStream::from(buffer);
        self.client
            .put_object()
            .bucket(bucket_name)
            .key(full_path.to_string_lossy())
            .body(parquet_string)
            .send()
            .await
            .map_or_else(
                |e| {
                    let error_str = format!(
                        "Unable to save {} in bucket {bucket_name}, {e}",
                        full_path.display()
                    );
                    self.project_logger.log_error(&error_str);
                    Err(AWSWriteFileError::SdkError(e))
                },
                |_| {
                    let debug_str =
                        format!("File {} saved in bucket {bucket_name}", full_path.display());
                    self.project_logger.log_debug(&debug_str);
                    Ok(())
                },
            )
    }

    pub async fn download_file(
        &self,
        bucket_name: &str,
        folder_path: &Path,
        file: &str,
        local_path: &Path,
        local_file: &str,
    ) -> Result<(), AWSLoadFileError> {
        let full_path = folder_path.join(file);
        let get_object = self
            .client
            .get_object()
            .bucket(bucket_name)
            .key(full_path.to_string_lossy())
            .send()
            .await
            .map_err(|e| {
                let error_str = format!(
                    "Unable to get the file {file} from folder {} in bucket {bucket_name}. {e}",
                    folder_path.display()
                );
                self.project_logger.log_error(&error_str);
                AWSLoadFileError::SdkError(e)
            });
        let byte = get_object?.body.collect().await.map_err(|e| {
            let error_str = format!(
                "Unable to read the file {file} from folder {} in bucket {bucket_name}. {e}",
                folder_path.display()
            );
            self.project_logger.log_error(&error_str);
            AWSLoadFileError::ByteStreamError(e)
        });
        let full_local_path = local_path.join(local_file);
        tokio::fs::write(&full_local_path, byte?.to_vec())
            .await
            .map_or_else(
                |e| {
                    let error_str =
                        format!("Unable to download to file {}. {e}", full_path.display());
                    self.project_logger.log_error(&error_str);
                    Err(AWSLoadFileError::IOError(e))
                },
                |_| {
                    let debug_str = format!("File {} downloaded.", full_path.display());
                    self.project_logger.log_debug(&debug_str);
                    Ok(())
                },
            )
    }

    pub async fn upload_file(
        &self,
        bucket_name: &str,
        folder_path: &Path,
        file: &str,
        local_path: &Path,
        local_file: &str,
    ) -> Result<(), AWSWriteFileError> {
        let full_local_path = local_path.join(local_file);
        let full_path = folder_path.join(file);
        let temp_file = File::open(&full_local_path).await.map_err(|e| {
            let error_str = format!(
                "Unable to open the local file {}. {e}",
                &full_local_path.display()
            );
            self.project_logger.log_error(&error_str);
            AWSWriteFileError::IOError(e)
        });
        let mut temp_file = temp_file?;
        let metadata = temp_file.metadata().await.map_err(|e| {
            let error_str = format!(
                "Unable to get the metadata for file {}. {e}",
                &full_local_path.display()
            );
            self.project_logger.log_error(&error_str);
            AWSWriteFileError::IOError(e)
        });
        let metadata = metadata?;
        if metadata.len() >= LIMIT_SINGLE_UPLOAD as u64 {
            self.upload_multipart(
                &mut temp_file,
                metadata.len() as usize,
                bucket_name,
                &full_path,
                &full_local_path,
            )
            .await
        } else {
            self.upload_single_object(&mut temp_file, bucket_name, &full_path, &full_local_path)
                .await
        }
    }

    async fn upload_single_object(
        &self,
        temp_file: &mut File,
        bucket_name: &str,
        full_path: &Path,
        full_local_path: &Path,
    ) -> Result<(), AWSWriteFileError> {
        let mut bytes = Vec::new();
        if let Err(e) = temp_file.read_to_end(&mut bytes).await {
            let error_str = format!(
                "Unable to read the local file {} as bytes. {e}",
                &full_local_path.display()
            );
            self.project_logger.log_error(&error_str);
            return Err(AWSWriteFileError::IOError(e));
        };
        let content = ByteStream::from(bytes);
        self.client
            .put_object()
            .bucket(bucket_name)
            .key(full_path.to_string_lossy())
            .body(content)
            .send()
            .await
            .map_or_else(
                |e| {
                    let error_str =
                        format!("Unable to upload to file {}. {e}", full_path.display());
                    self.project_logger.log_error(&error_str);
                    Err(AWSWriteFileError::SdkError(e))
                },
                |_| {
                    let debug_str = format!("File {} uploaded.", full_path.display());
                    self.project_logger.log_debug(&debug_str);
                    Ok(())
                },
            )
    }

    async fn upload_multipart(
        &self,
        temp_file: &mut File,
        file_size: usize,
        bucket_name: &str,
        full_path: &Path,
        full_local_path: &Path,
    ) -> Result<(), AWSWriteFileError> {
        let upload_id = self
            .client
            .create_multipart_upload()
            .bucket(bucket_name)
            .key(full_path.to_string_lossy())
            .send()
            .await
            .map_or_else(
                |e| {
                    let error_str = format!("Unable to create the multipart upload. {e}");
                    self.project_logger.log_error(&error_str);
                    Err(AWSWriteFileError::CreateMultipartUploadError(e))
                },
                |response| {
                    response.upload_id().map_or_else(
                        || {
                            let error_str = "No upload ID is generated from the multipart upload.";
                            self.project_logger.log_error(error_str);
                            Err(AWSWriteFileError::CreateMultipartUploadError(
                                SdkError::<CreateMultipartUploadError>::construction_failure(
                                    error_str,
                                ),
                            ))
                        },
                        |response| Ok(response.to_string()),
                    )
                },
            );
        let mut part_number = 1;
        let mut offset: u64 = 0;
        let mut completed_parts = CompletedMultipartUpload::builder();

        let upload_id = upload_id?;

        while offset < file_size as u64 {
            let mut part_data = vec![0; MULTIPART_SIZE];
            match temp_file.seek(SeekFrom::Start(offset)).await {
                Ok(_) => {
                    let bytes_to_read = if (file_size - offset as usize) < MULTIPART_SIZE {
                        temp_file.read_to_end(&mut part_data).await
                    } else {
                        temp_file.read_exact(&mut part_data).await
                    };
                    match bytes_to_read {
                        Ok(bytes_read) => {
                            if bytes_read == 0 {
                                break;
                            }
                            let content = ByteStream::from(part_data);
                            match self
                                .client
                                .upload_part()
                                .bucket(bucket_name)
                                .key(full_path.to_string_lossy())
                                .part_number(part_number)
                                .upload_id(&upload_id)
                                .body(content)
                                .send()
                                .await
                            {
                                Ok(uploaded_part) => {
                                    let e_tag = uploaded_part.e_tag().unwrap_or_else(|| {
                                        panic!(
                                            "Unable to find e-tag for file {} part {part_number}",
                                            full_path.display()
                                        )
                                    });
                                    let completed_part = CompletedPart::builder()
                                        .part_number(part_number)
                                        .e_tag(e_tag)
                                        .build();
                                    completed_parts = completed_parts.parts(completed_part);
                                    let debug_str = format!(
                                        "File {} part {part_number} uploaded.",
                                        full_path.display()
                                    );
                                    self.project_logger.log_debug(&debug_str);
                                }
                                Err(e) => {
                                    let error_str = format!(
                                        "Unable to upload the file {} part {part_number}. {e}",
                                        full_path.display()
                                    );
                                    self.project_logger.log_error(&error_str);
                                    return Err(AWSWriteFileError::UploadPartError(e));
                                }
                            };
                            offset += bytes_read as u64;
                            part_number += 1;
                        }
                        Err(e) => {
                            let error_str = format!(
                                "Unable to read the local file {} part {part_number} as bytes. {e}",
                                &full_local_path.display()
                            );
                            self.project_logger.log_error(&error_str);
                            return Err(AWSWriteFileError::IOError(e));
                        }
                    }
                }
                Err(e) => {
                    let error_str = format!(
                        "Unable to seek position for file {}. {e}",
                        full_local_path.display()
                    );
                    self.project_logger.log_error(&error_str);
                    return Err(AWSWriteFileError::IOError(e));
                }
            };
        }

        self.client
            .complete_multipart_upload()
            .bucket(bucket_name)
            .key(full_path.to_string_lossy())
            .upload_id(&upload_id)
            .multipart_upload(completed_parts.build())
            .send()
            .await
            .map_or_else(
                |e| {
                    let error_str = format!(
                        "Unable to upload to file {} multipart. {e}",
                        full_path.display()
                    );
                    self.project_logger.log_error(&error_str);
                    Err(AWSWriteFileError::CompleteMultipartUploadError(e))
                },
                |_| {
                    let debug_str = format!("File {} multipart uploaded.", full_path.display());
                    self.project_logger.log_debug(&debug_str);
                    Ok(())
                },
            )
    }

    pub async fn delete_file(&self, bucket_name: &str, folder_path: &Path, file: &str) {
        let full_path = folder_path.join(file);
        match self
            .client
            .delete_object()
            .bucket(bucket_name)
            .key(full_path.to_string_lossy())
            .send()
            .await
        {
            Ok(_) => {
                let debug_str = format!("File {} deleted.", full_path.display());
                self.project_logger.log_debug(&debug_str);
            }
            Err(e) => {
                let error_str = format!("Unable to delete to file {}. {e}", full_path.display());
                self.project_logger.log_error(&error_str);
                panic!("{}", &error_str);
            }
        };
    }

    pub async fn delete_folder(&self, bucket_name: &str, folder_path: &Path) {
        self.delete_file(bucket_name, folder_path, "").await;
    }
}

#[derive(Debug)]
pub enum AWSLoadFileError {
    SdkError(SdkError<GetObjectError>),
    ByteStreamError(aws_smithy_http::byte_stream::error::Error),
    PolarsError(PolarsError),
    IOError(std::io::Error),
}

impl From<SdkError<GetObjectError>> for AWSLoadFileError {
    fn from(err: SdkError<GetObjectError>) -> Self {
        AWSLoadFileError::SdkError(err)
    }
}

impl From<aws_smithy_http::byte_stream::error::Error> for AWSLoadFileError {
    fn from(err: aws_smithy_http::byte_stream::error::Error) -> Self {
        AWSLoadFileError::ByteStreamError(err)
    }
}

impl From<PolarsError> for AWSLoadFileError {
    fn from(err: PolarsError) -> Self {
        AWSLoadFileError::PolarsError(err)
    }
}

impl From<std::io::Error> for AWSLoadFileError {
    fn from(err: std::io::Error) -> Self {
        AWSLoadFileError::IOError(err)
    }
}

#[derive(Debug)]
pub enum AWSWriteFileError {
    SdkError(SdkError<PutObjectError>),
    PolarsError(PolarsError),
    IOError(std::io::Error),
    CreateMultipartUploadError(SdkError<CreateMultipartUploadError>),
    UploadPartError(SdkError<UploadPartError>),
    CompleteMultipartUploadError(SdkError<CompleteMultipartUploadError>),
}

impl From<SdkError<PutObjectError>> for AWSWriteFileError {
    fn from(err: SdkError<PutObjectError>) -> Self {
        AWSWriteFileError::SdkError(err)
    }
}

impl From<PolarsError> for AWSWriteFileError {
    fn from(err: PolarsError) -> Self {
        AWSWriteFileError::PolarsError(err)
    }
}

impl From<std::io::Error> for AWSWriteFileError {
    fn from(err: std::io::Error) -> Self {
        AWSWriteFileError::IOError(err)
    }
}

impl From<SdkError<CreateMultipartUploadError>> for AWSWriteFileError {
    fn from(err: SdkError<CreateMultipartUploadError>) -> Self {
        AWSWriteFileError::CreateMultipartUploadError(err)
    }
}

impl From<SdkError<UploadPartError>> for AWSWriteFileError {
    fn from(err: SdkError<UploadPartError>) -> Self {
        AWSWriteFileError::UploadPartError(err)
    }
}

impl From<SdkError<CompleteMultipartUploadError>> for AWSWriteFileError {
    fn from(err: SdkError<CompleteMultipartUploadError>) -> Self {
        AWSWriteFileError::CompleteMultipartUploadError(err)
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
        let bucket_name = "sctys";
        assert!(aws_file_io.check_bucket_exist(bucket_name).await);
        let bucket_name = "abc";
        assert!(!aws_file_io.check_bucket_exist(bucket_name).await);
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
        let bucket_name = "sctys";
        let folder_name = Path::new("data/poisson_football");
        assert!(
            aws_file_io
                .check_folder_exist(bucket_name, folder_name)
                .await
        );
        let folder_name = Path::new("abc");
        assert!(
            !aws_file_io
                .check_folder_exist(bucket_name, folder_name)
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
        let bucket_name = "sctys";
        let folder_name = Path::new("data/poisson_football/");
        let file_name = "test_list.html";
        assert!(
            aws_file_io
                .check_file_exist(bucket_name, folder_name, file_name)
                .await
        );
        let file_name = "abc";
        assert!(
            !aws_file_io
                .check_file_exist(bucket_name, folder_name, file_name)
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
        let bucket_name = "sctys";
        let folder_name = Path::new("data/test_folder/");
        aws_file_io
            .create_directory_if_not_exists(bucket_name, folder_name)
            .await
            .unwrap();
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
        let bucket_name = "sctys";
        let folder_name = Path::new("data/poisson_football");
        let elements = aws_file_io
            .get_elements_in_folder(bucket_name, folder_name)
            .await
            .unwrap();
        println!("{:?}", elements[elements.len() - 1].contents().unwrap());
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
        let bucket_name = "sctys";
        let folder_name = Path::new("data/poisson_football/");
        let file_name = "test_list.html";
        let content = aws_file_io
            .load_file_as_string(bucket_name, folder_name, file_name)
            .await
            .unwrap();
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
        let local_file = "test.html";
        let file_io = FileIO::new(&project_logger);
        let data = file_io
            .load_file_as_string(&local_folder_path, local_file)
            .unwrap();
        let bucket_name = "sctys";
        let folder_name = Path::new("data/test_folder/");
        let file_name = "test_aws.html";
        aws_file_io
            .write_string_to_file(bucket_name, folder_name, file_name, data.as_str())
            .await
            .unwrap();
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
        let bucket_name = "sctys";
        let folder_name = Path::new("data/test_folder/");
        let file_name = "test.csv";
        let content = aws_file_io
            .load_csv_file(bucket_name, folder_name, file_name)
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
        let local_file = "test_new.csv";
        let mut data = file_io
            .load_csv_file(&local_folder_path, local_file)
            .unwrap();
        let bucket_name = "sctys";
        let folder_name = Path::new("data/test_folder/");
        let file_name = "test_new_aws.csv";
        aws_file_io
            .write_csv_file(bucket_name, folder_name, file_name, &mut data)
            .await
            .unwrap();
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
        let local_file = "test.parquet";
        let mut data = file_io
            .load_parquet_file(&local_folder_path, local_file)
            .unwrap();
        let bucket_name = "sctys";
        let folder_name = Path::new("data/test_folder/");
        let file_name = "test_aws.parquet";
        aws_file_io
            .write_parquet_file(bucket_name, folder_name, file_name, &mut data)
            .await
            .unwrap();
        let content = aws_file_io
            .load_parquet_file(bucket_name, folder_name, file_name)
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
        let local_file = "test_aws.parquet";
        let bucket_name = "sctys";
        let folder_name = Path::new("data/test_folder/");
        let file_name = "test_aws.parquet";
        aws_file_io
            .download_file(
                bucket_name,
                folder_name,
                file_name,
                &local_folder_path,
                local_file,
            )
            .await
            .unwrap();
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
        let local_file = "test_scrape.html";
        let bucket_name = "sctys";
        let folder_name = Path::new("data/test_folder/");
        let file_name = "test_scrape_aws.html";
        aws_file_io
            .upload_file(
                bucket_name,
                folder_name,
                file_name,
                &local_folder_path,
                local_file,
            )
            .await
            .unwrap();
    }
}
